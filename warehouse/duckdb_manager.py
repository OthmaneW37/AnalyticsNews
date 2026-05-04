"""
warehouse/duckdb_manager.py
---------------------------
Data Warehouse local basé sur DuckDB.
Implémente le schéma analytique Gold + vues pour le dashboard.

Usage :
    from warehouse.duckdb_manager import DuckDBManager
    db = DuckDBManager()
    db.insert_gold_articles(gold_df)
    db.insert_ingestion_stats(stats)
    df = db.query("SELECT * FROM v_source_breakdown")
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(os.getenv("WAREHOUSE_PATH", "data/warehouse/news_warehouse.duckdb"))

# Schéma SQL synchronisé avec warehouse/models.sql
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gold_articles (
    article_id          VARCHAR PRIMARY KEY,
    url                 TEXT,
    titre_clean         TEXT,
    contenu_clean       TEXT,
    content_length      INTEGER,
    content_hash        VARCHAR(32),
    source              VARCHAR,
    raw_source          VARCHAR,
    langue              VARCHAR,
    langue_detectee     VARCHAR,
    pays                VARCHAR(3),
    date_publication    TIMESTAMP,
    auteur              VARCHAR,
    categorie           VARCHAR,
    quality_status      VARCHAR(4),
    quality_flags       TEXT,
    topic_id            INTEGER,
    topic_label         VARCHAR,
    topic_prob          FLOAT,
    topic_article_count INTEGER,
    topic_coverage_score FLOAT,
    polymarket_question  TEXT,
    polymarket_prob      FLOAT,
    polymarket_prob_pct  VARCHAR(10),
    polymarket_volume_usd FLOAT,
    polymarket_url       TEXT,
    combined_signal      FLOAT,
    processed_at        TIMESTAMP,
    gold_built_at       TIMESTAMP,
    ingested_at         TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_gold_topic_summary START 1;
CREATE TABLE IF NOT EXISTS gold_topic_summary (
    id                  INTEGER DEFAULT nextval('seq_gold_topic_summary') PRIMARY KEY,
    run_date            DATE,
    topic_label         VARCHAR,
    article_count       INTEGER,
    sources_json        TEXT,
    avg_polymarket_prob FLOAT,
    polymarket_question TEXT,
    date_min            TIMESTAMP,
    date_max            TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_ingestion_stats START 1;
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id              INTEGER DEFAULT nextval('seq_ingestion_stats') PRIMARY KEY,
    run_date        DATE,
    run_timestamp   TIMESTAMP,
    source          VARCHAR,
    articles_scraped INTEGER,
    quality_ok      INTEGER,
    quality_fail    INTEGER,
    elapsed_seconds FLOAT,
    bronze_path     TEXT,
    silver_path     TEXT
);

CREATE TABLE IF NOT EXISTS analytics_articles_by_day (
    pub_date    DATE PRIMARY KEY,
    article_count INTEGER,
    avg_content_length FLOAT,
    top_source  VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_articles_by_theme (
    topic_label VARCHAR PRIMARY KEY,
    article_count INTEGER,
    avg_polymarket_prob FLOAT,
    top_source  VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_articles_by_country (
    pays        VARCHAR(3) PRIMARY KEY,
    article_count INTEGER,
    top_source  VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_articles_by_source (
    source      VARCHAR PRIMARY KEY,
    article_count INTEGER,
    quality_ok_rate FLOAT,
    avg_content_length FLOAT,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW v_topic_daily_coverage AS
SELECT
    DATE(date_publication)    AS pub_date,
    topic_label,
    COUNT(*)                  AS article_count,
    AVG(polymarket_prob)      AS avg_market_prob,
    SUM(topic_coverage_score) AS total_coverage_score
FROM gold_articles
WHERE quality_status = 'OK'
  AND topic_id >= 0
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

CREATE OR REPLACE VIEW v_top_topics_with_signal AS
SELECT
    topic_label,
    COUNT(*)                          AS article_count,
    ROUND(AVG(polymarket_prob), 4)    AS avg_prob,
    MAX(polymarket_question)          AS market_question,
    ROUND(AVG(combined_signal), 4)    AS avg_combined_signal
FROM gold_articles
WHERE quality_status = 'OK'
  AND topic_id >= 0
  AND polymarket_prob IS NOT NULL
GROUP BY topic_label
ORDER BY article_count DESC, avg_prob DESC
LIMIT 20;

CREATE OR REPLACE VIEW v_source_breakdown AS
SELECT
    source,
    langue,
    pays,
    COUNT(*)                   AS article_count,
    MIN(date_publication)      AS earliest,
    MAX(date_publication)      AS latest,
    AVG(content_length)        AS avg_content_length
FROM gold_articles
WHERE quality_status = 'OK'
GROUP BY source, langue, pays
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_daily_stats AS
SELECT
    DATE(date_publication) AS pub_date,
    source,
    COUNT(*) AS article_count,
    AVG(content_length) AS avg_length,
    COUNT(DISTINCT topic_label) AS topic_count
FROM gold_articles
WHERE quality_status = 'OK'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
"""


class DuckDBManager:
    """
    Gère la connexion et les opérations sur le Data Warehouse DuckDB.
    """

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()
        logger.info(f"[Warehouse] DuckDB connecté → {self.db_path}")

    def _init_schema(self):
        """Crée les tables et vues si elles n'existent pas."""
        self.conn.execute(SCHEMA_SQL)

    # ------------------------------------------------------------------
    # INSERTIONS
    # ------------------------------------------------------------------

    def insert_gold_articles(self, df: pd.DataFrame):
        """Insère ou remplace les articles Gold dans la table gold_articles."""
        if df.empty:
            return
        df = df.copy()
        # Cast datetime columns
        for col in ["date_publication", "processed_at", "gold_built_at", "ingested_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        # Harmoniser colonnes avec le schéma
        expected = [
            "article_id", "url", "titre_clean", "contenu_clean", "content_length",
            "content_hash", "source", "raw_source", "langue", "langue_detectee",
            "pays", "date_publication", "auteur", "categorie", "quality_status",
            "quality_flags", "topic_id", "topic_label", "topic_prob",
            "topic_article_count", "topic_coverage_score",
            "polymarket_question", "polymarket_prob", "polymarket_prob_pct",
            "polymarket_volume_usd", "polymarket_url", "combined_signal",
            "processed_at", "gold_built_at", "ingested_at",
        ]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        # Dédoublonnage par article_id (keep last)
        df = df.drop_duplicates(subset=["article_id"], keep="last")
        # Upsert via DELETE + INSERT
        ids = tuple(df["article_id"].tolist())
        if len(ids) == 1:
            self.conn.execute("DELETE FROM gold_articles WHERE article_id = ?", (ids[0],))
        else:
            self.conn.execute(f"DELETE FROM gold_articles WHERE article_id IN {ids}")
        cols = ", ".join([f'"{c}"' for c in expected])
        self.conn.execute(f"INSERT INTO gold_articles ({cols}) SELECT {cols} FROM df")
        logger.info(f"[Warehouse] {len(df)} articles Gold insérés.")

    def insert_topic_summaries(self, summaries: list[dict], run_date: str = None):
        """Insère les résumés de topics."""
        if not summaries:
            return
        run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")
        rows = []
        for s in summaries:
            rows.append({
                "run_date": run_date,
                "topic_label": s.get("topic_label", ""),
                "article_count": s.get("article_count", 0),
                "sources_json": str(s.get("sources", {})),
                "avg_polymarket_prob": s.get("avg_polymarket_prob"),
                "polymarket_question": s.get("polymarket_question", ""),
                "date_min": s.get("date_range", {}).get("min", None),
                "date_max": s.get("date_range", {}).get("max", None),
            })
        df = pd.DataFrame(rows)
        cols = "run_date, topic_label, article_count, sources_json, avg_polymarket_prob, polymarket_question, date_min, date_max"
        self.conn.execute(f"INSERT INTO gold_topic_summary ({cols}) SELECT {cols} FROM df")
        logger.info(f"[Warehouse] {len(rows)} résumés de topics insérés.")

    def insert_ingestion_stats(self, stats: dict):
        """Insère les statistiques d'ingestion (dict {source: stats})."""
        if not stats:
            return
        rows = []
        run_ts = datetime.utcnow()
        run_date = run_ts.strftime("%Y-%m-%d")
        for source, s in stats.items():
            rows.append({
                "run_date": run_date,
                "run_timestamp": run_ts,
                "source": source,
                "articles_scraped": s.get("articles_scraped", 0),
                "quality_ok": s.get("quality_ok", 0),
                "quality_fail": s.get("quality_fail", 0),
                "elapsed_seconds": s.get("elapsed_seconds", 0.0),
                "bronze_path": s.get("bronze_path", ""),
                "silver_path": s.get("silver_path", ""),
            })
        df = pd.DataFrame(rows)
        cols = "run_date, run_timestamp, source, articles_scraped, quality_ok, quality_fail, elapsed_seconds, bronze_path, silver_path"
        self.conn.execute(f"INSERT INTO ingestion_stats ({cols}) SELECT {cols} FROM df")
        logger.info(f"[Warehouse] {len(rows)} stats d'ingestion insérées.")

    def refresh_analytics_tables(self):
        """
        Recalcule les tables analytiques à partir de gold_articles.
        """
        logger.info("[Warehouse] Refresh des tables analytiques...")

        # Articles par jour
        self.conn.execute("""
            DELETE FROM analytics_articles_by_day;
            INSERT INTO analytics_articles_by_day
            SELECT
                DATE(date_publication) AS pub_date,
                COUNT(*) AS article_count,
                AVG(content_length) AS avg_content_length,
                (SELECT source FROM gold_articles g2
                 WHERE DATE(g2.date_publication) = DATE(g1.date_publication)
                 GROUP BY source ORDER BY COUNT(*) DESC LIMIT 1) AS top_source,
                CURRENT_TIMESTAMP AS updated_at
            FROM gold_articles g1
            WHERE quality_status = 'OK'
            GROUP BY DATE(date_publication);
        """)

        # Articles par thème
        self.conn.execute("""
            DELETE FROM analytics_articles_by_theme;
            INSERT INTO analytics_articles_by_theme
            SELECT
                topic_label,
                COUNT(*) AS article_count,
                AVG(polymarket_prob) AS avg_polymarket_prob,
                (SELECT source FROM gold_articles g2
                 WHERE g2.topic_label = g1.topic_label
                 GROUP BY source ORDER BY COUNT(*) DESC LIMIT 1) AS top_source,
                CURRENT_TIMESTAMP AS updated_at
            FROM gold_articles g1
            WHERE quality_status = 'OK' AND topic_id >= 0
            GROUP BY topic_label;
        """)

        # Articles par pays
        self.conn.execute("""
            DELETE FROM analytics_articles_by_country;
            INSERT INTO analytics_articles_by_country
            SELECT
                pays,
                COUNT(*) AS article_count,
                (SELECT source FROM gold_articles g2
                 WHERE g2.pays = g1.pays
                 GROUP BY source ORDER BY COUNT(*) DESC LIMIT 1) AS top_source,
                CURRENT_TIMESTAMP AS updated_at
            FROM gold_articles g1
            WHERE quality_status = 'OK'
            GROUP BY pays;
        """)

        # Articles par source
        self.conn.execute("""
            DELETE FROM analytics_articles_by_source;
            INSERT INTO analytics_articles_by_source
            SELECT
                source,
                COUNT(*) AS article_count,
                ROUND(SUM(CASE WHEN quality_status = 'OK' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS quality_ok_rate,
                AVG(content_length) AS avg_content_length,
                CURRENT_TIMESTAMP AS updated_at
            FROM gold_articles
            GROUP BY source;
        """)

        logger.info("[Warehouse] Tables analytiques refresh terminé.")

    # ------------------------------------------------------------------
    # REQUÊTES
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Exécute une requête SQL et retourne un DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def get_top_keywords(self, n: int = 20) -> pd.DataFrame:
        """
        Retourne les mots les plus fréquents dans les titres (stopwords fr/en).
        """
        stopwords = {
            "le", "la", "les", "de", "des", "du", "un", "une", "et", "en", "à", "au", "aux",
            "est", "sont", "a", "ont", "dans", "pour", "par", "sur", "avec", "ce", "cet", "cette",
            "se", "que", "qui", "ne", "pas", "plus", "mais", "ou", "où", "son", "sa", "ses",
            "the", "of", "and", "to", "a", "in", "is", "it", "you", "that", "he", "was", "for",
            "on", "are", "as", "with", "his", "they", "i", "at", "be", "this", "have", "from",
            "or", "one", "had", "by", "word", "but", "not", "what", "all", "were", "we", "when",
            "your", "can", "said", "there", "use", "an", "each", "which", "she", "do", "how",
            "their", "if", "will", "up", "other", "about", "out", "many", "then", "them", "these",
            "so", "some", "her", "would", "make", "like", "into", "him", "has", "two", "more",
            "go", "no", "way", "could", "my", "than", "first", "been", "call", "who", "its",
            "now", "find", "long", "down", "day", "did", "get", "come", "made", "may", "part",
        }
        df = self.query("SELECT titre_clean FROM gold_articles WHERE quality_status = 'OK'")
        if df.empty or "titre_clean" not in df.columns:
            return pd.DataFrame(columns=["mot", "frequence"])
        words = []
        for title in df["titre_clean"].dropna().astype(str):
            for w in title.lower().split():
                w = w.strip(".,;:!?()[]{}\"'\n")
                if len(w) > 3 and w not in stopwords:
                    words.append(w)
        from collections import Counter
        counter = Counter(words)
        top = counter.most_common(n)
        return pd.DataFrame(top, columns=["mot", "frequence"])

    def close(self):
        self.conn.close()
        logger.info("[Warehouse] Connexion DuckDB fermée.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
