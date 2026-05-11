"""
test_duckdb_manager.py — Unit tests for DuckDBManager.
"""
import os
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

from warehouse.duckdb_manager import DuckDBManager


@pytest.fixture
def db_path(tmp_path):
    """Returns a temporary DuckDB file path."""
    return str(tmp_path / "test_warehouse.duckdb")


@pytest.fixture
def db(db_path):
    """Creates and tears down a DuckDBManager instance."""
    manager = DuckDBManager(db_path=db_path)
    yield manager
    manager.close()


@pytest.fixture
def sample_gold_df():
    """A minimal Gold DataFrame for insertion."""
    return pd.DataFrame({
        "article_id": ["a1", "a2", "a3"],
        "url": ["https://bbc.com/1", "https://bbc.com/2", "https://hespress.com/1"],
        "titre_clean": ["UK economy grows", "Brexit update", "Maroc reforme"],
        "contenu_clean": ["The UK economy has grown significantly this quarter with strong indicators", "Brexit negotiations continue with new developments expected soon", "Le Maroc a annonce une reforme economique importante pour stimuler la croissance"],
        "content_length": [80, 80, 85],
        "content_hash": ["h1", "h2", "h3"],
        "source": ["bbc.com", "bbc.com", "hespress.com"],
        "raw_source": ["bbc_rss", "bbc_rss", "hespress_rss"],
        "langue": ["en", "en", "fr"],
        "langue_detectee": ["en", "en", "fr"],
        "pays": ["GB", "GB", "MA"],
        "date_publication": pd.to_datetime(["2025-04-26", "2025-04-26", "2025-04-26"]),
        "auteur": ["John", "Jane", "Ahmed"],
        "categorie": ["Economy", "Politics", "Economy"],
        "quality_status": ["OK", "OK", "OK"],
        "quality_flags": ["[]", "[]", "[]"],
        "topic_id": [0, 0, 1],
        "topic_label": ["uk_economy", "uk_economy", "maroc_reform"],
        "topic_prob": [0.8, 0.7, 0.9],
        "topic_article_count": [2, 2, 1],
        "topic_coverage_score": [1.0, 1.0, 0.5],
        "polymarket_question": ["Will UK grow?", "Will UK grow?", "Will Maroc reform?"],
        "polymarket_prob": [0.65, 0.65, 0.40],
        "polymarket_prob_pct": ["65.0%", "65.0%", "40.0%"],
        "polymarket_volume_usd": [1000, 1000, 500],
        "polymarket_url": ["https://x.com/1", "https://x.com/1", "https://x.com/2"],
        "combined_signal": [0.52, 0.455, 0.36],
        "processed_at": pd.to_datetime(["2025-04-26T10:00:00", "2025-04-26T10:00:00", "2025-04-26T10:00:00"]),
        "gold_built_at": pd.to_datetime(["2025-04-26T10:05:00", "2025-04-26T10:05:00", "2025-04-26T10:05:00"]),
        "ingested_at": pd.to_datetime(["2025-04-26T09:55:00", "2025-04-26T09:55:00", "2025-04-26T09:55:00"]),
    })


class TestDuckDBManagerInit:
    def test_creates_database_file(self, tmp_path):
        db_path = str(tmp_path / "init_test.duckdb")
        manager = DuckDBManager(db_path=db_path)
        manager.close()
        assert Path(db_path).exists()

    def test_creates_parent_directory(self, tmp_path):
        db_path = str(tmp_path / "nested" / "dir" / "test.duckdb")
        manager = DuckDBManager(db_path=db_path)
        manager.close()
        assert Path(db_path).exists()

    def test_creates_tables(self, db):
        tables = db.query("SHOW TABLES")
        table_names = tables["name"].tolist()
        expected = [
            "gold_articles", "gold_topic_summary", "ingestion_stats",
            "analytics_articles_by_day", "analytics_articles_by_theme",
            "analytics_articles_by_country", "analytics_articles_by_source",
        ]
        for t in expected:
            assert t in table_names

    def test_creates_views(self, db):
        result = db.query("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'")
        view_names = result["table_name"].tolist()
        expected_views = [
            "v_topic_daily_coverage", "v_top_topics_with_signal",
            "v_source_breakdown", "v_daily_stats",
        ]
        for v in expected_views:
            assert v in view_names


class TestInsertGoldArticles:
    def test_inserts_rows(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        count = db.query("SELECT COUNT(*) as cnt FROM gold_articles")["cnt"].iloc[0]
        assert count == 3

    def test_upsert_by_article_id(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        same_df = sample_gold_df.copy()
        same_df["titre_clean"] = ["Updated 1", "Updated 2", "Updated 3"]
        db.insert_gold_articles(same_df)
        count = db.query("SELECT COUNT(*) as cnt FROM gold_articles")["cnt"].iloc[0]
        assert count == 3

    def test_insert_empty_df_no_error(self, db):
        db.insert_gold_articles(pd.DataFrame())

    def test_insert_missing_columns_fills_nulls(self, db):
        df = pd.DataFrame({
            "article_id": ["a1"],
            "url": ["https://x.com"],
            "titre_clean": ["Title"],
            "contenu_clean": ["Content is long enough for validation"],
            "content_length": [40],
            "content_hash": ["h1"],
            "source": ["test.com"],
            "langue": ["fr"],
            "pays": ["MA"],
            "date_publication": pd.to_datetime(["2025-01-01"]),
            "quality_status": ["OK"],
            "topic_id": [0],
            "topic_label": ["test"],
            "topic_prob": [0.5],
        })
        db.insert_gold_articles(df)
        count = db.query("SELECT COUNT(*) as cnt FROM gold_articles")["cnt"].iloc[0]
        assert count == 1


class TestInsertTopicSummaries:
    def test_inserts_summaries(self, db):
        summaries = [
            {
                "topic_label": "topic_a",
                "article_count": 5,
                "sources": {"bbc.com": 3, "hespress.com": 2},
                "avg_polymarket_prob": 0.65,
                "polymarket_question": "Will X happen?",
                "date_range": {"min": "2025-04-26", "max": "2025-04-26"},
            }
        ]
        db.insert_topic_summaries(summaries, run_date="2025-04-26")
        count = db.query("SELECT COUNT(*) as cnt FROM gold_topic_summary")["cnt"].iloc[0]
        assert count == 1

    def test_insert_empty_summaries_no_error(self, db):
        db.insert_topic_summaries([])


class TestInsertIngestionStats:
    def test_inserts_stats(self, db):
        stats = {
            "bbc": {"articles_scraped": 10, "quality_ok": 9, "quality_fail": 1, "elapsed_seconds": 5.2, "bronze_path": "data/bronze/bbc", "silver_path": "data/silver/bbc"},
            "hespress": {"articles_scraped": 8, "quality_ok": 7, "quality_fail": 1, "elapsed_seconds": 4.1, "bronze_path": "data/bronze/hespress", "silver_path": "data/silver/hespress"},
        }
        db.insert_ingestion_stats(stats)
        count = db.query("SELECT COUNT(*) as cnt FROM ingestion_stats")["cnt"].iloc[0]
        assert count == 2

    def test_insert_empty_stats_no_error(self, db):
        db.insert_ingestion_stats({})


class TestRefreshAnalytics:
    def test_refresh_populates_tables(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        db.refresh_analytics_tables()

        by_day = db.query("SELECT COUNT(*) as cnt FROM analytics_articles_by_day")["cnt"].iloc[0]
        assert by_day > 0

        by_theme = db.query("SELECT COUNT(*) as cnt FROM analytics_articles_by_theme")["cnt"].iloc[0]
        assert by_theme > 0

        by_country = db.query("SELECT COUNT(*) as cnt FROM analytics_articles_by_country")["cnt"].iloc[0]
        assert by_country > 0

        by_source = db.query("SELECT COUNT(*) as cnt FROM analytics_articles_by_source")["cnt"].iloc[0]
        assert by_source > 0


class TestQuery:
    def test_query_returns_dataframe(self, db):
        result = db.query("SELECT 1 as val")
        assert isinstance(result, pd.DataFrame)
        assert result["val"].iloc[0] == 1

    def test_query_gold_articles_after_insert(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        result = db.query("SELECT * FROM gold_articles WHERE quality_status = 'OK'")
        assert len(result) == 3


class TestViews:
    def test_v_source_breakdown(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        result = db.query("SELECT * FROM v_source_breakdown")
        assert len(result) > 0

    def test_v_daily_stats(self, db, sample_gold_df):
        db.insert_gold_articles(sample_gold_df)
        result = db.query("SELECT * FROM v_daily_stats")
        assert len(result) > 0
