"""
test_article.py — Unit tests for the Article class and BaseScraper.
"""
import hashlib
import pytest
from datetime import datetime

from scrapers.base_scraper import Article, BaseScraper


class TestArticle:
    def test_article_creation_minimal(self):
        a = Article(titre="Test", url="https://example.com/1", source="test.com", langue="fr")
        assert a.titre == "Test"
        assert a.url == "https://example.com/1"
        assert a.source == "test.com"
        assert a.langue == "fr"
        assert a.contenu == ""
        assert a.auteur == ""
        assert a.categorie == ""
        assert a.pays == "unknown"

    def test_article_creation_full(self):
        a = Article(
            titre="Full article",
            url="https://example.com/2",
            source="example.com",
            langue="en",
            date_publication="2025-04-26T10:00:00",
            contenu="Some content here",
            pays="US",
            raw_source="example_rss",
            auteur="Jane Doe",
            categorie="Politics",
        )
        assert a.titre == "Full article"
        assert a.contenu == "Some content here"
        assert a.pays == "US"
        assert a.auteur == "Jane Doe"
        assert a.categorie == "Politics"
        assert a.raw_source == "example_rss"
        assert a.date_publication == "2025-04-26T10:00:00"

    def test_article_id_is_deterministic(self):
        url = "https://example.com/unique-article"
        a1 = Article(titre="A", url=url, source="x.com", langue="fr")
        a2 = Article(titre="B", url=url, source="x.com", langue="fr")
        assert a1.article_id == a2.article_id
        expected = hashlib.md5(url.encode()).hexdigest()
        assert a1.article_id == expected

    def test_article_id_differs_per_url(self):
        a1 = Article(titre="A", url="https://example.com/1", source="x.com", langue="fr")
        a2 = Article(titre="A", url="https://example.com/2", source="x.com", langue="fr")
        assert a1.article_id != a2.article_id

    def test_to_dict_contains_all_keys(self):
        a = Article(
            titre="Test",
            url="https://example.com/3",
            source="test.com",
            langue="fr",
            date_publication="2025-01-01",
            contenu="Content",
            pays="MA",
            raw_source="test_rss",
            auteur="Author",
            categorie="Cat",
        )
        d = a.to_dict()
        expected_keys = {
            "article_id", "titre", "url", "source", "langue",
            "date_publication", "contenu", "pays", "raw_source",
            "auteur", "categorie", "ingested_at",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_values_match(self):
        a = Article(titre="Title", url="https://example.com/4", source="x.com", langue="en", contenu="Body")
        d = a.to_dict()
        assert d["titre"] == "Title"
        assert d["url"] == "https://example.com/4"
        assert d["contenu"] == "Body"
        assert d["source"] == "x.com"

    def test_to_dict_ingested_at_is_iso(self):
        a = Article(titre="T", url="https://example.com/5", source="x.com", langue="fr")
        d = a.to_dict()
        assert "ingested_at" in d
        datetime.fromisoformat(d["ingested_at"])

    def test_default_date_is_now(self):
        before = datetime.utcnow()
        a = Article(titre="T", url="https://example.com/6", source="x.com", langue="fr")
        after = datetime.utcnow()
        pub = datetime.fromisoformat(a.date_publication)
        assert before <= pub <= after


class TestBaseScraper:
    def test_concrete_scraper_run(self):
        class DummyScraper(BaseScraper):
            def fetch_articles(self):
                return [
                    Article(titre="A1", url="https://example.com/a1", source="dummy.com", langue="fr"),
                    Article(titre="A2", url="https://example.com/a2", source="dummy.com", langue="fr"),
                ]

        scraper = DummyScraper(name="dummy")
        result = scraper.run()
        assert len(result) == 2
        assert result[0]["titre"] == "A1"
        assert result[1]["titre"] == "A2"

    def test_concrete_scraper_run_empty(self):
        class EmptyScraper(BaseScraper):
            def fetch_articles(self):
                return []

        scraper = EmptyScraper(name="empty")
        result = scraper.run()
        assert result == []

    def test_concrete_scraper_run_exception(self):
        class FailingScraper(BaseScraper):
            def fetch_articles(self):
                raise RuntimeError("Network error")

        scraper = FailingScraper(name="failing")
        result = scraper.run()
        assert result == []

    def test_scraper_logger_name(self):
        class DummyScraper(BaseScraper):
            def fetch_articles(self):
                return []

        scraper = DummyScraper(name="my_source")
        assert scraper.name == "my_source"
