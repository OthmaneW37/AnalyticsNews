"""
test_bronze_writer.py — Unit tests for BronzeWriter (local mode).
"""
import json
import pytest
from pathlib import Path
from datetime import datetime

from datalake.bronze_writer import BronzeWriter


class TestBronzeWriterWrite:
    def test_write_creates_file(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="test", articles=[sample_article_dict])
        assert path is not None
        assert Path(path).exists()

    def test_write_creates_correct_partition(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="bbc", articles=[sample_article_dict])
        today = datetime.utcnow().strftime("%Y-%m-%d")
        expected_dir = Path(tmp_dir) / "bbc" / today
        assert expected_dir.exists()
        assert Path(path).parent == expected_dir

    def test_write_filename_pattern(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="bbc", articles=[sample_article_dict])
        today = datetime.utcnow().strftime("%Y-%m-%d")
        assert f"bbc_{today}_" in Path(path).name
        assert Path(path).name.endswith(".json")

    def test_write_json_content_is_valid(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="test", articles=[sample_article_dict])
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        assert "metadata" in payload
        assert "articles" in payload

    def test_write_metadata(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="hespress", articles=[sample_article_dict])
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        meta = payload["metadata"]
        assert meta["source"] == "hespress"
        assert meta["article_count"] == 1
        assert meta["schema_version"] == "1.0"
        assert "written_at" in meta

    def test_write_articles_content(self, tmp_dir, sample_article_dict):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="test", articles=[sample_article_dict])
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        assert len(payload["articles"]) == 1
        assert payload["articles"][0]["titre"] == sample_article_dict["titre"]

    def test_write_multiple_articles(self, tmp_dir, sample_articles_list):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="multi", articles=sample_articles_list)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        assert len(payload["articles"]) == 3
        assert payload["metadata"]["article_count"] == 3

    def test_write_empty_articles_returns_none(self, tmp_dir):
        writer = BronzeWriter(root=tmp_dir)
        path = writer.write(source="test", articles=[])
        assert path is None

    def test_write_creates_parents(self, tmp_dir, sample_article_dict):
        deep_root = Path(tmp_dir) / "deep" / "nested" / "path"
        writer = BronzeWriter(root=deep_root)
        path = writer.write(source="test", articles=[sample_article_dict])
        assert Path(path).exists()


class TestBronzeWriterRead:
    def test_read_latest_returns_articles(self, tmp_dir, sample_articles_list):
        writer = BronzeWriter(root=tmp_dir)
        writer.write(source="test", articles=sample_articles_list)
        articles = writer.read_latest(source="test")
        assert len(articles) == 3

    def test_read_latest_empty_dir(self, tmp_dir):
        writer = BronzeWriter(root=tmp_dir)
        articles = writer.read_latest(source="nonexistent")
        assert articles == []

    def test_read_all_returns_all_articles(self, tmp_dir, sample_articles_list):
        writer = BronzeWriter(root=tmp_dir)
        writer.write(source="test", articles=sample_articles_list[:1])
        import time
        time.sleep(1.1)
        writer.write(source="test", articles=sample_articles_list[1:])
        articles = writer.read_all(source="test")
        assert len(articles) == 3

    def test_read_all_empty_dir(self, tmp_dir):
        writer = BronzeWriter(root=tmp_dir)
        articles = writer.read_all(source="nonexistent")
        assert articles == []
