"""
conftest.py — Shared fixtures and sample data for all tests.
"""
import os
import sys
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def tmp_dir():
    """Creates a temporary directory that is cleaned up after the test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_article_dict():
    """A single valid article dict (Bronze format)."""
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        "titre": "Le Maroc annonce une nouvelle reforme economique",
        "url": "https://www.hespress.com/2025/04/26/reforme-economique",
        "source": "hespress.com",
        "langue": "fr",
        "date_publication": "2025-04-26T10:00:00",
        "contenu": "<p>Le gouvernement marocain a annonce aujourd'hui une nouvelle reforme economique visant a stimuler la croissance et attirer les investissements etrangers. Cette reforme comprend des mesures fiscales avantageuses pour les entreprises etrangers qui souhaitent s'installer au Maroc. Les secteurs de la technologie et des energies renouvelables sont particulierement concernes par cette initiative.</p>",
        "pays": "MA",
        "raw_source": "hespress_rss",
        "auteur": "Ahmed B.",
        "categorie": "Economie",
        "ingested_at": datetime.utcnow().isoformat(),
    }


@pytest.fixture
def sample_articles_list(sample_article_dict):
    """A list of 3 valid article dicts."""
    articles = [sample_article_dict]
    articles.append({
        "article_id": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
        "titre": "BBC reports on UK economic growth",
        "url": "https://www.bbc.com/news/uk-economy-2025",
        "source": "bbc.com",
        "langue": "en",
        "date_publication": "2025-04-26T11:00:00",
        "contenu": "<p>The UK economy has grown by 0.5 percent in the last quarter, according to official figures released today. The growth was driven by strong performance in the services sector and increased consumer spending. Analysts say this is a positive sign for the rest of the year.</p>",
        "pays": "GB",
        "raw_source": "bbc_rss",
        "auteur": "John Smith",
        "categorie": "Economy",
        "ingested_at": datetime.utcnow().isoformat(),
    })
    articles.append({
        "article_id": "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        "titre": "Al Jazeera covers Gaza ceasefire negotiations",
        "url": "https://www.aljazeera.com/news/2025/04/26/gaza-ceasefire",
        "source": "aljazeera.com",
        "langue": "en",
        "date_publication": "2025-04-26T12:00:00",
        "contenu": "<p>Negotiations for a ceasefire in Gaza have entered a new phase with mediators from Egypt and Qatar playing a key role. The discussions focus on humanitarian aid delivery and the release of prisoners. Both sides have expressed cautious optimism about reaching an agreement in the coming weeks.</p>",
        "pays": "QA",
        "raw_source": "aljazeera_rss",
        "auteur": "",
        "categorie": "Middle East",
        "ingested_at": datetime.utcnow().isoformat(),
    })
    return articles


@pytest.fixture
def sample_bad_articles():
    """Articles with quality issues."""
    return [
        {
            "article_id": "bad1",
            "titre": "ab",
            "url": "",
            "source": "test.com",
            "langue": "fr",
            "date_publication": None,
            "contenu": "too short",
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        },
        {
            "article_id": "bad2",
            "titre": "",
            "url": "not-a-url",
            "source": "test.com",
            "langue": "fr",
            "date_publication": "2025-04-26",
            "contenu": "x" * 10,
            "pays": "MA",
            "raw_source": "test",
            "auteur": "",
            "categorie": "",
            "ingested_at": datetime.utcnow().isoformat(),
        },
    ]


@pytest.fixture
def sample_duplicate_articles(sample_article_dict):
    """List with duplicate article_id."""
    dup = sample_article_dict.copy()
    dup["titre"] = "Modified title for duplicate"
    return [sample_article_dict, dup]
