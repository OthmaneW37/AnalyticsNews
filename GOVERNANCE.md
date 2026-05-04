# Gouvernance des données — News Pipeline

## 1. Data Catalog

### Catalog des sources

| Source | Type | Langue | Pays | Méthode | Fréquence |
|--------|------|--------|------|---------|-----------|
| Hespress | RSS + HTML | FR/AR | MA | RSS scraping | Batch / Streaming |
| Akhbarona | RSS + HTML | FR | MA | RSS scraping | Batch / Streaming |
| Lakom | RSS + HTML | FR | MA | RSS scraping | Batch / Streaming |
| Barlamane | RSS + HTML | FR | MA | RSS scraping | Batch / Streaming |
| BBC News | RSS + HTML | EN | GB | RSS scraping | Batch / Streaming |
| Al Jazeera | RSS + HTML | EN | QA | RSS scraping | Batch / Streaming |
| CNN | RSS + HTML | EN | US | RSS scraping | Batch / Streaming |
| Reuters | RSS + HTML | EN | US | RSS scraping | Batch / Streaming |
| GDELT v2 | API REST | Multilingue | Varié | API REST | Batch / Streaming |

### Catalog des champs (Article)

| Champ | Type | Description | Exemple |
|-------|------|-------------|---------|
| `article_id` | VARCHAR(32) | Hash MD5 de l'URL | `d41d8cd98f00b204e9800998ecf8427e` |
| `titre` | TEXT | Titre original | `"Maroc : croissance économique"` |
| `titre_clean` | TEXT | Titre nettoyé | `"Maroc croissance économique"` |
| `url` | TEXT | Lien permanent | `https://www.hespress.com/...` |
| `source` | VARCHAR | Domaine normalisé | `hespress.com` |
| `raw_source` | VARCHAR | Identifiant scraper | `hespress_rss` |
| `langue` | VARCHAR(2) | Langue déclarée | `fr` |
| `langue_detectee` | VARCHAR(2) | Langue détectée (langdetect) | `fr` |
| `pays` | VARCHAR(3) | Code pays ISO | `MA` |
| `date_publication` | TIMESTAMP | Date de publication | `2025-04-26T10:00:00` |
| `contenu` | TEXT | Contenu brut | `<p>Le Maroc...</p>` |
| `contenu_clean` | TEXT | Contenu nettoyé | `Le Maroc...` |
| `content_length` | INTEGER | Longueur contenu | 1245 |
| `content_hash` | VARCHAR(32) | Hash MD5 contenu | `...` |
| `auteur` | VARCHAR | Auteur de l'article | `Ahmed B.` |
| `categorie` | VARCHAR | Catégorie / section | `Economie` |
| `quality_status` | VARCHAR(4) | OK ou FAIL | `OK` |
| `quality_flags` | TEXT (JSON) | Liste des problèmes | `["CONTENU_TROP_COURT"]` |
| `topic_id` | INTEGER | ID BERTopic | `3` |
| `topic_label` | VARCHAR | Label du topic | `3_maroc_economie_croissance` |
| `topic_prob` | FLOAT | Probabilité topic | `0.82` |
| `polymarket_prob` | FLOAT | Probabilité marché | `0.68` |
| `combined_signal` | FLOAT | Signal combiné | `0.55` |
| `ingested_at` | TIMESTAMP | Date d'ingestion | `2025-04-26T12:00:00` |
| `processed_at` | TIMESTAMP | Date traitement Silver | `2025-04-26T12:01:00` |
| `gold_built_at` | TIMESTAMP | Date construction Gold | `2025-04-26T12:05:00` |

## 2. Data Lineage (Traçabilité)

```
Source Web (RSS/HTML/API)
    │
    ▼
[Scraper] ──► article.to_dict() ──► [BronzeWriter]
    │                                      │
    │                              data/bronze/<source>/<date>/
    │                              <source>_<date>_<time>.json
    │                                      │
    ▼                                      ▼
[KafkaProducer]                  [SilverProcessor]
news_events                      process()
    │                                      │
    │                              data/silver/<source>/<date>/
    │                              <source>_<date>_<time>.parquet
    │                                      │
    ▼                                      ▼
[KafkaConsumer]                  [GoldAggregator]
    │                              build_gold()
    │                                      │
    │                              data/gold/<date>/
    │                              gold_<date>_<time>.parquet
    │                                      │
    └──────────────────────────────────────┘
                    │
                    ▼
            [DuckDBManager]
            insert_gold_articles()
            refresh_analytics_tables()
                    │
                    ▼
            data/warehouse/news_warehouse.duckdb
                    │
                    ▼
            [Dashboard Streamlit]
            Lecture vues SQL + DataFrame
```

## 3. Contrôle qualité par dimension

### Complétude
Vérification que tous les champs obligatoires sont présents :
- Titre (≥ 5 caractères)
- Contenu (≥ 50 caractères)
- URL (non vide)
- Date (parsable)
- Auteur (non vide)
- Catégorie (non vide)

### Cohérence
- Langue déclarée (`langue`) vs langue détectée (`langue_detectee`) via `langdetect`
- Pays vs source (ex: `hespress.com` doit avoir `pays = MA`)

### Validité
- URL bien formée (regex `^https?://`)
- Date au format ISO 8601
- `content_hash` unique (détection de doublons de contenu)

## 4. Métadonnées de pipeline

Chaque exécution du pipeline produit des métadonnées :

```json
{
  "run_date": "2025-04-26",
  "run_timestamp": "2025-04-26T12:00:00",
  "source": "bbc",
  "articles_scraped": 25,
  "quality_ok": 23,
  "quality_fail": 2,
  "elapsed_seconds": 45.2,
  "bronze_path": "data/bronze/bbc/2025-04-26/bbc_2025-04-26_120000.json",
  "silver_path": "data/silver/bbc/2025-04-26/bbc_2025-04-26_120000.parquet"
}
```

Ces métadonnées sont stockées dans la table `ingestion_stats` du Data Warehouse.

## 5. Sécurité et accès

- **MinIO** : Authentification via `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`
- **Airflow** : Authentification basique (admin/admin par défaut, à changer en prod)
- **Kafka** : Mode PLAINTEXT (à sécuriser avec SASL/SSL en production)
- **DuckDB** : Fichier local (droits système uniquement)

## 6. Rétention et archivage

- **Bronze** : Conservé 90 jours (nettoyage automatique possible via DAG)
- **Silver** : Conservé 90 jours
- **Gold** : Conservé 365 jours
- **Warehouse** : Données Gold conservées indéfiniment (DuckDB compacte)

## 7. Documentation des changements

| Version | Date | Changement |
|---------|------|------------|
| 1.0 | 2025-04-26 | Phase 1 — Scrapers + Bronze + Silver |
| 2.0 | 2025-04-26 | Phase 2 — MinIO + Kafka + Airflow |
| 3.0 | 2025-04-26 | Phase 3 — BERTopic + Polymarket + Gold |
| 4.0 | 2025-04-26 | Phase 4 — Dashboard Streamlit + Data Warehouse DuckDB + Nouvelles sources |
