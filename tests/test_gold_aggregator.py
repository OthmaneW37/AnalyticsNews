"""
test_gold_aggregator.py — Unit tests for GoldAggregator and PolymarketEnricher.
"""
import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path

from datalake.gold_aggregator import GoldAggregator, PolymarketEnricher


class TestPolymarketEnricher:
    def test_enrich_dataframe_adds_columns(self):
        enricher = PolymarketEnricher()
        df = pd.DataFrame({
            "topic_label": ["topic_a", "topic_b", "topic_c"],
            "titre_clean": ["A", "B", "C"],
        })
        signals = {
            "topic_a": {"market_question": "Q1?", "probability": 0.7, "probability_pct": "70.0%", "volume_usd": 1000, "url": "https://x.com"},
            "topic_b": {"market_question": "Q2?", "probability": 0.3, "probability_pct": "30.0%", "volume_usd": 500, "url": "https://y.com"},
        }
        result = enricher.enrich_dataframe(df, signals)
        assert "polymarket_question" in result.columns
        assert "polymarket_prob" in result.columns
        assert "polymarket_prob_pct" in result.columns
        assert "polymarket_volume_usd" in result.columns
        assert "polymarket_url" in result.columns

    def test_enrich_dataframe_maps_correct_values(self):
        enricher = PolymarketEnricher()
        df = pd.DataFrame({
            "topic_label": ["topic_a"],
            "titre_clean": ["A"],
        })
        signals = {
            "topic_a": {"market_question": "Will X happen?", "probability": 0.68, "probability_pct": "68.0%", "volume_usd": 2000, "url": "https://polymarket.com"},
        }
        result = enricher.enrich_dataframe(df, signals)
        assert result["polymarket_question"].iloc[0] == "Will X happen?"
        assert result["polymarket_prob"].iloc[0] == 0.68
        assert result["polymarket_prob_pct"].iloc[0] == "68.0%"

    def test_enrich_dataframe_missing_signal_is_none(self):
        enricher = PolymarketEnricher()
        df = pd.DataFrame({
            "topic_label": ["unknown_topic"],
            "titre_clean": ["A"],
        })
        result = enricher.enrich_dataframe(df, {})
        assert pd.isna(result["polymarket_prob"].iloc[0])

    def test_enrich_dataframe_empty_df(self):
        enricher = PolymarketEnricher()
        df = pd.DataFrame()
        result = enricher.enrich_dataframe(df, {})
        assert result.empty

    def test_enrich_dataframe_no_topic_column(self):
        enricher = PolymarketEnricher()
        df = pd.DataFrame({"other": [1, 2]})
        result = enricher.enrich_dataframe(df, {})
        assert "polymarket_prob" not in result.columns

    def test_fetch_market_signals_filters_invalid_keywords(self):
        enricher = PolymarketEnricher()
        signals = enricher.fetch_market_signals(["hors-sujet", "-1_noise", "", None])
        assert len(signals) == 0


class TestGoldAggregatorBuildGold:
    def _make_silver_df(self):
        return pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "titre_clean": ["Title 1", "Title 2", "Title 3"],
            "contenu_clean": ["Content one is long enough", "Content two is long enough", "Content three is long enough"],
            "source": ["bbc", "bbc", "hespress"],
            "langue": ["en", "en", "fr"],
            "pays": ["GB", "GB", "MA"],
            "date_publication": pd.to_datetime(["2025-04-26", "2025-04-26", "2025-04-26"]),
            "quality_status": ["OK", "OK", "FAIL"],
            "topic_label": ["politics_election", "politics_election", "economy_maroc"],
            "topic_id": [0, 0, 1],
            "topic_prob": [0.8, 0.7, 0.9],
            "url": ["https://bbc.com/1", "https://bbc.com/2", "https://hespress.com/1"],
        })

    def test_build_gold_filters_quality(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = self._make_silver_df()
        result = aggregator.build_gold(df, enrich_polymarket=False)
        assert len(result) == 2
        assert (result["quality_status"] == "OK").all()

    def test_build_gold_empty_df(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        result = aggregator.build_gold(pd.DataFrame(), enrich_polymarket=False)
        assert result.empty

    def test_build_gold_adds_coverage_score(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = self._make_silver_df()
        result = aggregator.build_gold(df, enrich_polymarket=False)
        assert "topic_coverage_score" in result.columns
        assert "topic_article_count" in result.columns

    def test_build_gold_coverage_score_max_is_1(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = self._make_silver_df()
        result = aggregator.build_gold(df, enrich_polymarket=False)
        assert result["topic_coverage_score"].max() == 1.0

    def test_build_gold_adds_gold_built_at(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = self._make_silver_df()
        result = aggregator.build_gold(df, enrich_polymarket=False)
        assert "gold_built_at" in result.columns


class TestGoldAggregatorTopicSummary:
    def test_topic_summary_returns_list(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = pd.DataFrame({
            "topic_label": ["topic_a", "topic_a", "topic_b"],
            "source": ["bbc", "bbc", "hespress"],
            "date_publication": pd.to_datetime(["2025-04-26", "2025-04-26", "2025-04-26"]),
        })
        result = aggregator.get_topic_summary(df)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_topic_summary_has_correct_keys(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = pd.DataFrame({
            "topic_label": ["topic_a"],
            "source": ["bbc"],
            "date_publication": pd.to_datetime(["2025-04-26"]),
        })
        result = aggregator.get_topic_summary(df)
        summary = result[0]
        assert "topic_label" in summary
        assert "article_count" in summary
        assert "sources" in summary
        assert "avg_polymarket_prob" in summary
        assert "polymarket_question" in summary
        assert "date_range" in summary

    def test_topic_summary_sorted_by_count(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        df = pd.DataFrame({
            "topic_label": ["small", "big", "big", "big"],
            "source": ["bbc", "bbc", "bbc", "bbc"],
            "date_publication": pd.to_datetime(["2025-04-26"] * 4),
        })
        result = aggregator.get_topic_summary(df)
        assert result[0]["topic_label"] == "big"
        assert result[0]["article_count"] == 3
        assert result[1]["topic_label"] == "small"
        assert result[1]["article_count"] == 1

    def test_topic_summary_empty_df(self):
        aggregator = GoldAggregator(use_minio=False, use_duckdb=False)
        result = aggregator.get_topic_summary(pd.DataFrame())
        assert result == []


class TestGoldAggregatorSaveAndLoad:
    def _make_gold_df(self):
        return pd.DataFrame({
            "article_id": ["a1"],
            "titre_clean": ["Title"],
            "contenu_clean": ["Content"],
            "source": ["bbc"],
            "langue": ["en"],
            "pays": ["GB"],
            "date_publication": pd.to_datetime(["2025-04-26"]),
            "quality_status": ["OK"],
            "topic_label": ["topic_a"],
            "topic_id": [0],
            "topic_prob": [0.8],
            "url": ["https://bbc.com/1"],
        })

    def test_save_creates_parquet(self, tmp_dir):
        aggregator = GoldAggregator(gold_root=tmp_dir, use_minio=False, use_duckdb=False)
        df = self._make_gold_df()
        path = aggregator.save(df)
        assert path is not None
        assert str(path).endswith(".parquet")

    def test_save_creates_topics_json(self, tmp_dir):
        aggregator = GoldAggregator(gold_root=tmp_dir, use_minio=False, use_duckdb=False)
        df = self._make_gold_df()
        summaries = [{"topic_label": "topic_a", "article_count": 1}]
        aggregator.save(df, topic_summaries=summaries)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        json_files = list((Path(tmp_dir) / today).glob("topics_*.json"))
        assert len(json_files) == 1

    def test_save_empty_df_returns_none(self, tmp_dir):
        aggregator = GoldAggregator(gold_root=tmp_dir, use_minio=False, use_duckdb=False)
        result = aggregator.save(pd.DataFrame())
        assert result is None

    def test_load_returns_dataframe(self, tmp_dir):
        aggregator = GoldAggregator(gold_root=tmp_dir, use_minio=False, use_duckdb=False)
        df = self._make_gold_df()
        aggregator.save(df)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        loaded = aggregator.load(date=today)
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 1

    def test_load_empty_dir_returns_empty(self, tmp_dir):
        aggregator = GoldAggregator(gold_root=tmp_dir, use_minio=False, use_duckdb=False)
        result = aggregator.load(date="2000-01-01")
        assert result.empty
