# News Pipeline — Architecture de données

Pipeline de collecte et traitement d'articles de presse multilingue
avec architecture Bronze / Silver / Gold et Data Warehouse DuckDB.

---

## Structure du projet

```
news-pipeline/
├── scrapers/
│   ├── base_scraper.py        ← Contrat abstrait (Article, BaseScraper)
│   ├── hespress_scraper.py    ← Hespress (MA)
│   ├── akhbarona_scraper.py   ← Akhbarona (MA)
│   ├── lakom_scraper.py       ← Lakom (MA)
│   ├── barlamane_scraper.py   ← Barlamane (MA)
│   ├── bbc_scraper.py         ← BBC News (GB)
│   ├── aljazeera_scraper.py   ← Al Jazeera (QA)
│   ├── cnn_scraper.py         ← CNN (US)
│   ├── reuters_scraper.py     ← Reuters (US)
│   └── gdelt_client.py        ← GDELT v2 (Multilingue)
│
├── datalake/
│   ├── bronze_writer.py       ← Écriture JSON brut (Bronze)
│   ├── silver_processor.py   ← Nettoyage + qualité + langdetect (Silver)
│   └── gold_aggregator.py    ← Agrégation BERTopic + Polymarket (Gold)
│
├── ingestion/
│   ├── batch_ingestion.py    ← Orchestrateur CLI batch
│   ├── kafka_producer.py     ← Events Kafka (streaming)
│   └── kafka_consumer.py     ← Consumer temps réel
│
├── warehouse/
│   ├── models.sql            ← Schéma SQL analytique
│   └── duckdb_manager.py     ← Data Warehouse local (DuckDB)
│
├── api_server.py             ← API FastAPI (exposition données + pipeline + frontend)
│
├── frontend/                 ← Dashboard Next.js 16 + TypeScript + Tailwind
│   ├── src/app/              ← Pages (Dashboard, Pipeline, Articles, Sujets, Qualité)
│   ├── src/lib/api.ts        ← Client API typé (TanStack Query)
│   └── dist/                 ← Build statique servi par FastAPI
│
├── orchestration/dags/
│   ├── batch_dag.py          ← DAG Airflow ingestion
│   └── quality_dag.py        ← DAG Airflow qualité + Gold
│
├── data/                     ← Généré automatiquement au runtime
│   ├── bronze/<source>/<date>/
│   ├── silver/<source>/<date>/
│   ├── gold/<date>/
│   └── warehouse/
│
├── test_phase1.py            ← Smoke test rapide
├── run_full_pipeline.py      ← Pipeline complet Phases 1→4
├── docker-compose.yml        ← Infrastructure distribuée
├── requirements.txt
├── README.md
├── ARCHITECTURE.md           ← Schéma et flux de données
└── GOVERNANCE.md             ← Catalog, lineage, qualité
```

---

## Installation

```bash
pip install -r requirements.txt
```

> **Note** : `torch` doit être installé séparément selon votre plateforme :
> ```bash
> # CPU only
> pip install torch --index-url https://download.pytorch.org/whl/cpu
> ```

---

## Utilisation

### Test rapide (5 articles BBC)
```bash
python -X utf8 test_phase1.py --source bbc --max 5
```

### Test avec une source marocaine
```bash
python -X utf8 test_phase1.py --source hespress --max 10
```

### Test avec GDELT
```bash
python -X utf8 test_phase1.py --source gdelt --query "Maroc économie"
```

### Pipeline complet local (toutes les sources)
```bash
python -X utf8 run_full_pipeline.py --source bbc hespress gdelt akhbarona lakom barlamane aljazeera cnn reuters --max-per-feed 10
```

### Pipeline batch (CLI)
```bash
python -X utf8 -m ingestion.batch_ingestion --sources bbc hespress --max-per-feed 10
```

### Mode rapide (sans fetch_content)
```bash
python -X utf8 -m ingestion.batch_ingestion --no-content --max-per-feed 10
```

---

## Phase 2 : Architecture Distribuée (Docker)

> Docker ne fonctionne pas sur ce PC ? Pas de problème. Tout fonctionne **en local sans Docker**. Le `docker-compose.yml` est prêt à être transféré sur un autre PC.

### Lancer l'infrastructure (sur un PC avec Docker)

```bash
docker-compose up -d
```

Services démarrés :
1. `news_minio` — MinIO (API 9000, Console 9001)
2. `news_minio_setup` — Création auto des buckets bronze/silver/gold
3. `news_kafka` — Broker Kafka (9092)
4. `news_postgres` — PostgreSQL pour Airflow
5. `news_airflow_web` / `news_airflow_scheduler` — Airflow (8080)
6. `news_dashboard` — API FastAPI (8000)

### Interfaces Web

- **Dashboard Next.js** : http://localhost:8000
- **MinIO Console** : http://localhost:9001 (admin / password)
- **Airflow Web UI** : http://localhost:8080 (admin / admin)
- **API FastAPI Docs** : http://localhost:8000/docs

---

## Architecture des couches

| Couche | Format | Description |
|--------|--------|-------------|
| **Bronze** | JSON | Données brutes, S3 `s3://bronze/source/date/` |
| **Silver** | Parquet + JSON | Données nettoyées, détection langue, qualité |
| **Gold** | Parquet + JSON | Agrégats BERTopic + signaux Polymarket + tables analytiques |
| **Warehouse** | DuckDB | Tables analytiques SQL + vues |


---

## Contrôle qualité Silver

Trois dimensions contrôlées :

| Dimension | Flags possibles |
|-----------|-----------------|
| **Complétude** | `TITRE_VIDE_OU_TROP_COURT`, `CONTENU_TROP_COURT`, `URL_MANQUANTE`, `DATE_MANQUANTE`, `AUTEUR_MANQUANT`, `CATEGORIE_MANQUANTE` |
| **Validité** | `URL_INVALIDE` |
| **Cohérence** | `LANGUE_INCOHERENTE` (déclarée vs détectée) |

---

## Data Warehouse (DuckDB)

Le Data Warehouse est un fichier DuckDB local : `data/warehouse/news_warehouse.duckdb`.

### Tables principales

- `gold_articles` — Articles enrichis (Gold)
- `gold_topic_summary` — Résumés par topic
- `ingestion_stats` — Stats d'ingestion par run
- `analytics_articles_by_day` — Agrégation quotidienne
- `analytics_articles_by_theme` — Agrégation par topic
- `analytics_articles_by_country` — Agrégation par pays
- `analytics_articles_by_source` — Agrégation par source

### Vues

- `v_topic_daily_coverage`
- `v_top_topics_with_signal`
- `v_source_breakdown`
- `v_daily_stats`

---



## Dashboard Next.js

Le frontend est une application **Next.js 16** avec App Router, servie directement par l'API FastAPI.

### Stack

- Next.js 16 + React 19 + TypeScript strict
- Tailwind CSS v4 (dark mode uniquement)
- TanStack Query (polling auto 30s dashboard, 2s logs)
- Recharts (graphiques)
- Framer Motion (animations)
- Lucide React (icônes)

### Pages

| Page | URL | Description |
|------|-----|-------------|
| **Dashboard** | `/` | KPIs, graphiques, live feed, top sujets |
| **Pipeline** | `/pipeline` | Lancer le pipeline, terminal de logs temps réel |
| **Articles** | `/articles` | Table filtrable, drawer de détail |
| **Sujets** | `/topics` | Bubble chart BERTopic + signaux Polymarket |
| **Qualité** | `/quality` | Funnel, taux OK/FAIL par source |

### Build du frontend

```bash
cd frontend
npm install
npm run build
```

Le build statique est généré dans `frontend/dist` et servi automatiquement par `api_server.py` sur `http://localhost:8000`.

### Développement

```bash
cd frontend
npm run dev
```

Le frontend en dev se connecte à l'API sur `http://localhost:8000`.

---

## Sources supportées

| Source | Langue | Méthode | Pays |
|--------|--------|---------|------|
| Hespress | FR/AR | RSS + HTML | MA |
| Akhbarona | FR | RSS + HTML | MA |
| Lakom | FR | RSS + HTML | MA |
| Barlamane | FR | RSS + HTML | MA |
| BBC News | EN | RSS + HTML | GB |
| Al Jazeera | EN | RSS + HTML | QA |
| CNN | EN | RSS + HTML | US |
| Reuters | EN | RSS + HTML | US |
| GDELT v2 | Multilingue | API REST | Varié |

---

## Livrables

- [x] Présentation détaillée du projet (README + ARCHITECTURE.md)
- [x] Schéma d'architecture (ARCHITECTURE.md)
- [x] Code source versionné sur Git
- [x] Fichiers de déploiement (docker-compose.yml)
- [x] Documentation technique (README, ARCHITECTURE.md, GOVERNANCE.md)
- [x] Dashboard Next.js professionnel (frontend/)
- [x] Démonstration fonctionnelle du pipeline (test_phase1.py, run_full_pipeline.py)

---

## Roadmap

| Phase | Composants | Statut |
|-------|-----------|--------|
| **Phase 1** | Scrapers + Bronze + Silver (Local) | ✅ DONE |
| **Phase 2** | MinIO + Kafka + Airflow (Docker) | ✅ DONE |
| **Phase 3** | BERTopic + Polymarket + Gold | ✅ DONE |
| **Phase 4** | Dashboard Next.js + Data Warehouse DuckDB | ✅ DONE |
