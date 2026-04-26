# News Pipeline — Architecture de données

Pipeline de collecte et traitement d'articles de presse multilingue
avec architecture Bronze / Silver / Gold.

---

## Structure du projet

```
news-pipeline/
├── scrapers/
│   ├── base_scraper.py        ← Contrat abstrait (Article, BaseScraper)
│   ├── hespress_scraper.py    ← Scraper Hespress via RSS
│   ├── bbc_scraper.py         ← Scraper BBC News via RSS
│   └── gdelt_client.py        ← Client GDELT v2 (Différenciateur #1)
│
├── datalake/
│   ├── bronze_writer.py       ← Écriture JSON brut (Bronze)
│   └── silver_processor.py   ← Nettoyage + qualité (Silver)
│
├── ingestion/
│   └── batch_ingestion.py    ← Orchestrateur CLI Phase 1
│
├── data/                     ← Généré automatiquement au runtime
│   ├── bronze/<source>/<date>/
│   └── silver/<source>/<date>/
│
├── test_phase1.py            ← Smoke test de validation
└── requirements.txt
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Utilisation

### Test rapide (5 articles BBC)
```bash
python -X utf8 test_phase1.py --source bbc --max 5
```

### Test avec GDELT
```bash
python -X utf8 test_phase1.py --source gdelt --query "Maroc économie"
```

### Test avec Hespress
```bash
python -X utf8 test_phase1.py --source hespress --max 10
```

### Pipeline complet (toutes les sources)
```bash
python -X utf8 -m ingestion.batch_ingestion
```

### Pipeline filtré
```bash
python -X utf8 -m ingestion.batch_ingestion --sources hespress gdelt --gdelt-query "Gaza" --gdelt-lang french
```

### Mode rapide (sans fetch_content, idéal pour les tests)
```bash
python -X utf8 -m ingestion.batch_ingestion --no-content --max-per-feed 10
```

---

## Phase 2 : Architecture Distribuée (Docker)

Cette phase ajoute MinIO (Data Lake local), Kafka (Streaming des événements), et Apache Airflow (Orchestration).

### Lancer l'infrastructure

Assurez-vous que **Docker Desktop** est lancé, puis exécutez :

```bash
docker-compose up -d
```

Cette commande va démarrer 5 conteneurs :
1. `news_minio` : MinIO (Port 9000: API, 9001: Console Web)
2. `news_minio_setup` : Création auto des buckets (`bronze`, `silver`, `gold`)
3. `news_kafka` : Broker Kafka (Port 9092)
4. `news_postgres` : Base de données pour Airflow
5. `news_airflow_web` et `news_airflow_scheduler` : Orchestrateur Airflow (Port 8080)

### Tester MinIO et Kafka en local

Vous pouvez lancer le script Python localement en activant MinIO et Kafka :

```bash
python -X utf8 -m ingestion.batch_ingestion --use-minio --use-kafka --source bbc
```

### Accéder aux Interfaces Web

- **MinIO Console** : [http://localhost:9001](http://localhost:9001) (User: `admin`, Password: `password`)
- **Airflow Web UI** : [http://localhost:8080](http://localhost:8080) (User: `admin`, Password: `admin`)

Depuis l'interface Airflow, vous pouvez activer le DAG `news_batch_ingestion` pour qu'il s'exécute automatiquement toutes les 2 heures.

---

## Architecture des couches

| Couche | Format | Description |
|--------|--------|-------------|
| **Bronze** | JSON | Données brutes, S3 `s3://bronze/source/date/` |
| **Silver** | Parquet + JSON | Données nettoyées, S3 `s3://silver/source/date/` |
| **Gold** | *(Phase 3)* | Agrégats BERTopic + signaux Polymarket |

---

## Contrôle qualité Silver

Chaque article passe une batterie de contrôles :

| Flag | Critère |
|------|---------|
| `TITRE_VIDE_OU_TROP_COURT` | Titre < 5 caractères |
| `CONTENU_TROP_COURT` | Contenu < 50 caractères après nettoyage |
| `URL_MANQUANTE` | Champ `url` vide |
| `DATE_MANQUANTE` | Date non parsable |

---

## Roadmap

| Phase | Composants | Statut |
|-------|-----------|--------|
| **Phase 1** | Scrapers + Bronze + Silver (Local) | ✅ **DONE** |
| **Phase 2** | MinIO + Kafka + Airflow (Docker) | ✅ **DONE** |
| **Phase 3** | BERTopic + Polymarket | ⏳ À venir |
| **Phase 4** | Dashboard Streamlit | ⏳ À venir |

---

## Sources supportées

| Source | Langue | Méthode | Diff. |
|--------|--------|---------|-------|
| Hespress | FR/AR | RSS + HTML | — |
| BBC News | EN | RSS + HTML | — |
| GDELT v2 | Multilingue | API REST | **#1** |
