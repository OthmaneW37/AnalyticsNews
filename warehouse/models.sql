-- ============================================================
-- warehouse/models.sql
-- Définition du schéma Gold pour DuckDB / PostgreSQL
-- ============================================================

-- ============================================================
-- Table principale : articles Gold enrichis
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_articles (
    -- Identifiants
    article_id          VARCHAR PRIMARY KEY,
    url                 TEXT,

    -- Contenu
    titre_clean         TEXT,
    contenu_clean       TEXT,
    content_length      INTEGER,
    content_hash        VARCHAR(32),

    -- Métadonnées source
    source              VARCHAR,
    raw_source          VARCHAR,
    langue              VARCHAR,
    pays                VARCHAR(3),
    date_publication    TIMESTAMP,

    -- Qualité
    quality_status      VARCHAR(4),   -- 'OK' ou 'FAIL'
    quality_flags       TEXT,         -- liste de flags JSON

    -- BERTopic (Différenciateur #2)
    topic_id            INTEGER,
    topic_label         VARCHAR,
    topic_prob          FLOAT,
    topic_article_count INTEGER,
    topic_coverage_score FLOAT,

    -- Polymarket (Différenciateur #3)
    polymarket_question  TEXT,
    polymarket_prob      FLOAT,
    polymarket_prob_pct  VARCHAR(10),
    polymarket_volume_usd FLOAT,
    polymarket_url       TEXT,
    combined_signal      FLOAT,

    -- Timestamps de traitement
    processed_at        TIMESTAMP,
    gold_built_at       TIMESTAMP,
    ingested_at         TIMESTAMP
);

-- ============================================================
-- Table de résumé des topics (vue agrégée pour le dashboard)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_topic_summary (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE,
    topic_label         VARCHAR,
    article_count       INTEGER,
    sources_json        TEXT,         -- JSON : {"bbc.co.uk": 5, "hespress.com": 3}
    avg_polymarket_prob FLOAT,
    polymarket_question TEXT,
    date_min            TIMESTAMP,
    date_max            TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Table des sources (stats d'ingestion par source)
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id              SERIAL PRIMARY KEY,
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

-- ============================================================
-- Vues utiles pour le dashboard
-- ============================================================

-- Vue : couverture médiatique quotidienne par topic
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

-- Vue : top topics avec signal Polymarket
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

-- Vue : répartition des articles par source et langue
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
