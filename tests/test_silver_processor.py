"""
test_silver_processor.py — Comprehensive unit tests for SilverProcessor.
"""
import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path

from datalake.silver_processor import SilverProcessor


class TestCleanText:
    def test_removes_html_tags(self):
        result = SilverProcessor._clean_text("<p>Hello <b>world</b></p>")
        assert "<p>" not in result
        assert "<b>" not in result
        assert "Hello" in result

    def test_removes_urls(self):
        result = SilverProcessor._clean_text("Visit https://example.com/page for info")
        assert "https://example.com/page" not in result

    def test_removes_www_urls(self):
        result = SilverProcessor._clean_text("Go to www.example.com now")
        assert "www.example.com" not in result

    def test_removes_html_entities(self):
        result = SilverProcessor._clean_text("Rock &amp; Roll &nbsp; fun")
        assert "&amp;" not in result
        assert "&nbsp;" not in result

    def test_removes_numeric_entities(self):
        result = SilverProcessor._clean_text("Price &#163;50")
        assert "&#163;" not in result

    def test_removes_control_characters(self):
        result = SilverProcessor._clean_text("Hello\x00\x01\x02World")
        assert "\x00" not in result
        assert "\x01" not in result

    def test_collapses_multiple_spaces(self):
        result = SilverProcessor._clean_text("Hello    world")
        assert "  " not in result

    def test_collapses_excessive_newlines(self):
        result = SilverProcessor._clean_text("Line1\n\n\n\nLine2")
        assert "\n\n\n" not in result

    def test_strips_whitespace(self):
        result = SilverProcessor._clean_text("  hello  ")
        assert result == "hello"

    def test_empty_string(self):
        assert SilverProcessor._clean_text("") == ""

    def test_none_input(self):
        assert SilverProcessor._clean_text(None) == ""

    def test_non_string_input(self):
        assert SilverProcessor._clean_text(123) == ""

    def test_preserves_normal_text(self):
        text = "Le Maroc a annonce une nouvelle reforme"
        result = SilverProcessor._clean_text(text)
        assert result == text


class TestIsUrlValid:
    def test_valid_http(self):
        assert SilverProcessor._is_url_valid("http://example.com") is True

    def test_valid_https(self):
        assert SilverProcessor._is_url_valid("https://example.com/path?q=1") is True

    def test_invalid_empty(self):
        assert SilverProcessor._is_url_valid("") is False

    def test_invalid_none(self):
        assert SilverProcessor._is_url_valid(None) is False

    def test_invalid_no_protocol(self):
        assert SilverProcessor._is_url_valid("example.com") is False

    def test_invalid_just_protocol(self):
        assert SilverProcessor._is_url_valid("https://") is False


class TestDetectLanguage:
    def test_detects_french(self):
        text = "Le gouvernement francais a annonce aujourd'hui une nouvelle reforme economique majeure pour stimuler la croissance du pays"
        result = SilverProcessor._detect_language(text)
        assert result == "fr"

    def test_detects_english(self):
        text = "The government has announced a major economic reform to stimulate growth and attract foreign investment"
        result = SilverProcessor._detect_language(text)
        assert result == "en"

    def test_returns_unknown_for_short_text(self):
        assert SilverProcessor._detect_language("hi") == "unknown"

    def test_returns_unknown_for_empty(self):
        assert SilverProcessor._detect_language("") == "unknown"

    def test_returns_unknown_for_none(self):
        assert SilverProcessor._detect_language(None) == "unknown"


class TestProcess:
    def test_process_empty_list(self):
        processor = SilverProcessor()
        result = processor.process([])
        assert result.empty

    def test_process_returns_dataframe(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert isinstance(result, pd.DataFrame)

    def test_process_cleans_content(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert "<p>" not in result["contenu_clean"].iloc[0]
        assert "</p>" not in result["contenu_clean"].iloc[0]

    def test_process_cleans_title(self, sample_article_dict):
        sample_article_dict["titre"] = "<h1>Test Title</h1>"
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert "<h1>" not in result["titre_clean"].iloc[0]

    def test_process_detects_language(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert "langue_detectee" in result.columns
        assert result["langue_detectee"].iloc[0] != "unknown"

    def test_process_quality_status_ok(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert result["quality_status"].iloc[0] == "OK"
        assert result["quality_flags"].iloc[0] == []

    def test_process_quality_status_fail(self, sample_bad_articles):
        processor = SilverProcessor()
        result = processor.process(sample_bad_articles)
        assert (result["quality_status"] == "FAIL").all()

    def test_process_quality_flags_not_empty_for_bad(self, sample_bad_articles):
        processor = SilverProcessor()
        result = processor.process(sample_bad_articles)
        for flags in result["quality_flags"]:
            assert len(flags) > 0

    def test_process_deduplicates(self, sample_duplicate_articles):
        processor = SilverProcessor()
        result = processor.process(sample_duplicate_articles)
        assert len(result) == 1

    def test_process_normalizes_source(self, sample_article_dict):
        sample_article_dict["source"] = "  HESPRESS.COM  "
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert result["source"].iloc[0] == "hespress.com"

    def test_process_normalizes_langue(self, sample_article_dict):
        sample_article_dict["langue"] = "  FR  "
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert result["langue"].iloc[0] == "fr"

    def test_process_normalizes_pays(self, sample_article_dict):
        sample_article_dict["pays"] = "  ma  "
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert result["pays"].iloc[0] == "MA"

    def test_process_parses_date(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert pd.notna(result["date_publication"].iloc[0])

    def test_process_computes_content_length(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert result["content_length"].iloc[0] > 0

    def test_process_computes_content_hash(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert len(result["content_hash"].iloc[0]) == 32

    def test_process_has_processed_at(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        assert "processed_at" in result.columns
        assert result["processed_at"].iloc[0] != ""

    def test_process_multiple_articles(self, sample_articles_list):
        processor = SilverProcessor()
        result = processor.process(sample_articles_list)
        assert len(result) == 3

    def test_process_quality_flag_short_title(self):
        article = {
            "article_id": "x1",
            "titre": "ab",
            "url": "https://example.com",
            "source": "test.com",
            "langue": "fr",
            "date_publication": "2025-01-01",
            "contenu": "This is a sufficiently long content to pass the minimum threshold for validation",
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        }
        processor = SilverProcessor()
        result = processor.process([article])
        flags = result["quality_flags"].iloc[0]
        assert "TITRE_VIDE_OU_TROP_COURT" in flags

    def test_process_quality_flag_short_content(self):
        article = {
            "article_id": "x2",
            "titre": "A good title here",
            "url": "https://example.com",
            "source": "test.com",
            "langue": "fr",
            "date_publication": "2025-01-01",
            "contenu": "short",
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        }
        processor = SilverProcessor()
        result = processor.process([article])
        flags = result["quality_flags"].iloc[0]
        assert "CONTENU_TROP_COURT" in flags

    def test_process_quality_flag_missing_url(self):
        article = {
            "article_id": "x3",
            "titre": "A good title here",
            "url": "",
            "source": "test.com",
            "langue": "fr",
            "date_publication": "2025-01-01",
            "contenu": "This is a sufficiently long content to pass the minimum threshold for validation",
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        }
        processor = SilverProcessor()
        result = processor.process([article])
        flags = result["quality_flags"].iloc[0]
        assert "URL_MANQUANTE" in flags

    def test_process_quality_flag_invalid_url(self):
        article = {
            "article_id": "x4",
            "titre": "A good title here",
            "url": "not-a-url",
            "source": "test.com",
            "langue": "fr",
            "date_publication": "2025-01-01",
            "contenu": "This is a sufficiently long content to pass the minimum threshold for validation",
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        }
        processor = SilverProcessor()
        result = processor.process([article])
        flags = result["quality_flags"].iloc[0]
        assert "URL_INVALIDE" in flags

    def test_process_column_order(self, sample_article_dict):
        processor = SilverProcessor()
        result = processor.process([sample_article_dict])
        expected_cols = [
            "article_id", "titre_clean", "url", "source", "langue",
            "langue_detectee", "pays", "date_publication", "contenu_clean",
            "content_length", "content_hash", "raw_source", "auteur",
            "categorie", "quality_flags", "quality_status", "processed_at",
        ]
        assert list(result.columns) == expected_cols


class TestSaveAndLoad:
    def test_save_creates_parquet(self, tmp_dir, sample_article_dict):
        processor = SilverProcessor(silver_root=tmp_dir)
        df = processor.process([sample_article_dict])
        path = processor.save(df, source="test")
        assert path is not None
        assert str(path).endswith(".parquet")

    def test_save_creates_json(self, tmp_dir, sample_article_dict):
        processor = SilverProcessor(silver_root=tmp_dir)
        df = processor.process([sample_article_dict])
        processor.save(df, source="test")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        json_files = list((Path(tmp_dir) / "test" / today).glob("*.json"))
        assert len(json_files) == 1

    def test_save_empty_df_returns_none(self, tmp_dir):
        processor = SilverProcessor(silver_root=tmp_dir)
        result = processor.save(pd.DataFrame(), source="test")
        assert result is None

    def test_load_returns_dataframe(self, tmp_dir, sample_article_dict):
        processor = SilverProcessor(silver_root=tmp_dir)
        df = processor.process([sample_article_dict])
        processor.save(df, source="test")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        loaded = processor.load(source="test", date=today)
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 1

    def test_load_empty_dir_returns_empty(self, tmp_dir):
        processor = SilverProcessor(silver_root=tmp_dir)
        result = processor.load(source="nonexistent")
        assert result.empty
