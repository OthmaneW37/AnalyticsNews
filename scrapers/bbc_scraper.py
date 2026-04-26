"""
bbc_scraper.py
--------------
Scraper pour BBC News (flux RSS publics).
Couvre plusieurs catégories en anglais + la section Afrique.

Dépendances : requests, beautifulsoup4, feedparser
"""

import time
import logging
from datetime import datetime
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import Article, BaseScraper

logger = logging.getLogger(__name__)

# Flux RSS BBC disponibles sans authentification
BBC_FEEDS = {
    "world":   "http://feeds.bbci.co.uk/news/world/rss.xml",
    "africa":  "http://feeds.bbci.co.uk/news/world/africa/rss.xml",
    "business": "http://feeds.bbci.co.uk/news/business/rss.xml",
    "technology": "http://feeds.bbci.co.uk/news/technology/rss.xml",
    "science": "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NewsPipelineBot/1.0; "
        "+https://github.com/your-org/news-pipeline)"
    )
}


class BBCScraper(BaseScraper):
    """
    Scrape les articles BBC News via leurs flux RSS officiels.

    Paramètres
    ----------
    categories : list[str]
        Clés des catégories à scraper (voir BBC_FEEDS).
        Défaut : toutes les catégories disponibles.
    max_per_feed : int
        Articles max par flux (défaut 25).
    fetch_content : bool
        Récupère le texte complet de l'article (défaut True).
    delay : float
        Délai entre requêtes en secondes (défaut 0.5 — BBC tolère mieux).
    """

    def __init__(
        self,
        categories: Optional[list[str]] = None,
        max_per_feed: int = 25,
        fetch_content: bool = True,
        delay: float = 0.5,
    ):
        super().__init__(name="bbc")
        self.categories = categories or list(BBC_FEEDS.keys())
        self.max_per_feed = max_per_feed
        self.fetch_content = fetch_content
        self.delay = delay

    # ------------------------------------------------------------------
    # Méthode principale
    # ------------------------------------------------------------------

    def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []
        seen_urls: set[str] = set()

        for cat in self.categories:
            feed_url = BBC_FEEDS.get(cat)
            if not feed_url:
                self.logger.warning(f"Catégorie inconnue : {cat}")
                continue
            self.logger.info(f"Lecture du flux BBC [{cat}] : {feed_url}")
            results = self._parse_feed(feed_url, cat, seen_urls)
            articles.extend(results)

        return articles

    # ------------------------------------------------------------------
    # Parsing RSS
    # ------------------------------------------------------------------

    def _parse_feed(self, feed_url: str, category: str, seen_urls: set) -> list[Article]:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as exc:
            self.logger.warning(f"Impossible de lire {feed_url} : {exc}")
            return []

        results = []
        for entry in feed.entries[: self.max_per_feed]:
            url = entry.get("link", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            titre = entry.get("title", "").strip()
            date_raw = entry.get("published", "")
            date_pub = self._normalize_date(date_raw)
            summary = BeautifulSoup(
                entry.get("summary", ""), "html.parser"
            ).get_text(separator=" ").strip()

            contenu = summary
            if self.fetch_content:
                full = self._fetch_full_content(url)
                contenu = full if full else summary

            article = Article(
                titre=titre,
                url=url,
                source="bbc.co.uk",
                langue="en",
                date_publication=date_pub,
                contenu=contenu,
                pays="GB",
                raw_source=f"bbc_rss_{category}",
            )
            results.append(article)
            time.sleep(self.delay)

        return results

    # ------------------------------------------------------------------
    # Extraction contenu BBC
    # ------------------------------------------------------------------

    def _fetch_full_content(self, url: str) -> Optional[str]:
        """
        BBC structure ses articles avec des blocs <div data-component="text-block">.
        On concatene tous ces blocs pour obtenir l'article complet.
        """
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Sélecteurs BBC modernes (2024+)
            text_blocks = soup.select('div[data-component="text-block"] p')
            if text_blocks:
                return "\n".join(p.get_text(strip=True) for p in text_blocks)

            # Fallback sélecteurs anciens
            for sel in ["article", "div.story-body__inner", "main"]:
                tag = soup.select_one(sel)
                if tag:
                    for unwanted in tag.select("aside, nav, script, style, figure"):
                        unwanted.decompose()
                    text = tag.get_text(separator="\n").strip()
                    if len(text) > 100:
                        return text

            return None

        except Exception as exc:
            self.logger.debug(f"Erreur de récupération contenu BBC ({url}): {exc}")
            return None

    # ------------------------------------------------------------------
    # Normalisation de date
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_date(raw: str) -> str:
        if not raw:
            return datetime.utcnow().isoformat()
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return raw
