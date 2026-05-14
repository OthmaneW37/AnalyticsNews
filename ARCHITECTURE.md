# Architecture du projet — News Pipeline

## Vue d'ensemble

Le projet est une plateforme Big Data de collecte, stockage, transformation et visualisation d'articles de presse multilingues. Elle s'appuie sur une architecture distribuée moderne avec les couches Bronze / Silver / Gold (Médaillon).

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              SOURCES                                    │
│  Maroc : Hespress, Akhbarona, Lakom, Barlamane                          │
│  Int'l : BBC, Al Jazeera, CNN, Reuters, GDELT v2                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         INGESTION                                       │
│  Batch (Airflow DAGs)  +  Streaming (Kafka Producer/Consumer)          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
            ┌──────────────┐                ┌──────────────┐
            │   BRONZE     │                │    KAFKA     │
            │  JSON brut   │                │   Events     │
            │  (MinIO/S3)  │                │  (streaming) │
            └──────────────┘                └──────────────┘
                    │                               │
                    ▼                               ▼
            ┌──────────────┐                ┌──────────────┐
            │   SILVER     │◄───────────────┤  Consumer    │
            │  Parquet/JSON│                │  temps réel  │
            │  (nettoyé)   │                └──────────────┘
            └──────────────┘
                    │
                    ▼
            ┌──────────────┐
            │    GOLD      │
            │  Parquet/JSON│
            │  (enrichi)   │
            └──────────────┘
                    │
                    ▼
            ┌──────────────┐
            │  Data WH     │
            │   DuckDB     │
            │ (analytique) │
            └──────────────┘
                    │
                    ▼
            ┌──────────────┐
            │  API FastAPI │
            │  ( données ) │
            └──────────────┘
```

## Couches de données

### Bronze — Données brutes
- **Format** : JSON
- **Stockage** : Local (`data/bronze`) ou MinIO (`s3://bronze`)
- **Contenu** : Articles bruts sortis des scrapers (titre, url, source, langue, date, contenu, pays, auteur, catégorie)
- **Partitionnement** : `bronze/<source>/<date>/<source>_<date>_<time>.json`

### Silver — Données nettoyées
- **Format** : Parquet + JSON
- **Stockage** : Local (`data/silver`) ou MinIO (`s3://silver`)
- **Traitements** :
  - Suppression HTML, URLs, caractères de contrôle
  - Normalisation des champs (source, langue, pays)
  - Détection automatique de la langue (`langdetect`)
  - Déduplication par `article_id`
  - Contrôle qualité (complétude, cohérence, validité)

### Gold — Données enrichies
- **Format** : Parquet + JSON
- **Stockage** : Local (`data/gold`) ou MinIO (`s3://gold`)
- **Enrichissements** :
  - BERTopic (topic modeling multilingue)
  - Polymarket (signaux marché prédictif)
  - Score de couverture et signal combiné
- **Tables analytiques** :
  - Articles par jour
  - Articles par thème (topic)
  - Articles par pays
  - Articles par source

### Data Warehouse — DuckDB
- **Type** : Base analytique embarquée (fichier `.duckdb`)
- **Schéma** : `gold_articles`, `gold_topic_summary`, `ingestion_stats`
- **Vues** :
  - `v_topic_daily_coverage`
  - `v_top_topics_with_signal`
  - `v_source_breakdown`
  - `v_daily_stats`
- **Tables analytiques matérialisées** :
  - `analytics_articles_by_day`
  - `analytics_articles_by_theme`
  - `analytics_articles_by_country`
  - `analytics_articles_by_source`

## Composants techniques

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Scrapers | Python + BeautifulSoup + feedparser | Collecte web (RSS + HTML) |
| Ingestion Batch | Python CLI + Apache Airflow | Orchestration périodique |
| Ingestion Streaming | Kafka (Producer + Consumer) | Événements temps réel |
| Data Lake | MinIO (S3-compatible) ou Local | Stockage objet |
| Traitement | Pandas + PyArrow | Nettoyage, transformation |
| Topic Modeling | BERTopic + sentence-transformers | Détection thèmes |
| Enrichissement | Polymarket API | Signaux marché |
| Data Warehouse | DuckDB | Analytics SQL |
| Orchestration | Apache Airflow | DAGs batch + qualité |

| Conteneurisation | Docker + docker-compose | Déploiement portable |

## Flux de données détaillé

1. **Scraping** : Les scrapers récupèrent les articles via RSS et HTML.
2. **Bronze** : Les articles bruts sont sérialisés en JSON et partitionnés par source/date.
3. **Event Kafka** : Un événement est émis par source ingérée (nombre d'articles, chemin Bronze).
4. **Silver** : Le processor nettoie, détecte la langue, contrôle la qualité et sauvegarde en Parquet.
5. **Gold** : L'aggregator applique BERTopic et Polymarket, puis génère les tables analytiques.
6. **Warehouse** : Les données Gold et les stats d'ingestion sont insérées dans DuckDB.
7. **API** : L'API FastAPI (`/api/data`) expose les données Silver/Gold et les KPIs.

## Qualité des données

Trois dimensions sont contrôlées :

| Dimension | Critères |
|-----------|----------|
| **Complétude** | Titre, contenu, URL, date, auteur, catégorie présents |
| **Cohérence** | Langue déclarée vs langue détectée (`langdetect`) |
| **Validité** | URL bien formée, date parsable |

Flags possibles : `TITRE_VIDE_OU_TROP_COURT`, `CONTENU_TROP_COURT`, `URL_MANQUANTE`, `URL_INVALIDE`, `DATE_MANQUANTE`, `AUTEUR_MANQUANT`, `CATEGORIE_MANQUANTE`, `LANGUE_INCOHERENTE`.

## Scalabilité & Portabilité

- **Local** : Tout fonctionne sans Docker (DuckDB en local, fichiers JSON/Parquet).
- **Docker** : `docker-compose up -d` démarre MinIO, Kafka, Airflow, PostgreSQL, Dashboard React (via FastAPI).
- **Kubernetes** : `kubectl apply -k k8s/` déploie l'ensemble des services avec StatefulSets, PersistentVolumeClaims, Ingress.

### Kubernetes (manifests dans `k8s/`)

```bash
# Build de l'image Docker
docker build -t news-pipeline-api:latest .

# Déploiement complet
kubectl apply -k k8s/

# Vérification
kubectl get pods -n news-pipeline
kubectl get svc -n news-pipeline
```

Services K8s :
- `minio` — StatefulSet 1 replica, PVC 20Gi, ports 9000/9001
- `kafka` — StatefulSet 1 replica (KRaft), PVC 10Gi, port 9092
- `postgres` — StatefulSet 1 replica, PVC 10Gi, port 5432
- `airflow-webserver` — Deployment 1 replica, port 8080
- `airflow-scheduler` — Deployment 1 replica
- `dashboard-api` — Deployment 1 replica, PVC data + HF cache, Ingress sur `news-pipeline.local`
