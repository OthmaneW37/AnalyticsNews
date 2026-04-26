"""
hespress_scraper.py
-------------------
Scraper pour Hespress (https://www.hespress.com) — grand média marocain FR/AR.
Stratégie : lecture du flux RSS principal + scraping du contenu de chaque article.

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

# Flux RSS publics de Hespress (catégories les plus actives)
HESPRESS_FEEDS = [
    "https://www.hespress.com/feed",                  # fil général
    "https://www.hespress.com/politique/feed",
    "https://www.hespress.com/societe/feed",
    "https://www.hespress.com/economie/feed",
    "https://www.hespress.com/sport/feed",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


class HepressScraper(BaseScraper):
    """
    Scrape les articles Hespress via RSS + extraction HTML du corps.

    Paramètres
    ----------
    max_per_feed : int
        Nombre maximum d'articles à extraire par flux RSS (défaut 20).
    fetch_content : bool
        Si True, récupère le contenu complet de chaque article (plus lent).
        Si False, utilise uniquement le résumé RSS (rapide).
    delay : float
        Délai (secondes) entre deux requêtes HTTP pour ne pas spammer le serveur.
    """

    def __init__(
        self,
        max_per_feed: int = 20,
        fetch_content: bool = True,
        delay: float = 1.0,
    ):
        super().__init__(name="hespress")
        self.max_per_feed = max_per_feed
        self.fetch_content = fetch_content
        self.delay = delay

    # ------------------------------------------------------------------
    # Méthode principale (obligatoire — contrat BaseScraper)
    # ------------------------------------------------------------------

    def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []
        seen_urls: set[str] = set()  # déduplication

        for feed_url in HESPRESS_FEEDS:
            self.logger.info(f"Lecture du flux : {feed_url}")
            feed_articles = self._parse_feed(feed_url, seen_urls)
            articles.extend(feed_articles)

        return articles

    # ------------------------------------------------------------------
    # Parsing RSS
    # ------------------------------------------------------------------

    def _parse_feed(self, feed_url: str, seen_urls: set) -> list[Article]:
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

            # Optionnel : récupération du corps complet
            contenu = summary
            if self.fetch_content:
                full = self._fetch_full_content(url)
                contenu = full if full else summary

            article = Article(
                titre=titre,
                url=url,
                source="hespress.com",
                langue="fr",            # Hespress publie surtout en FR
                date_publication=date_pub,
                contenu=contenu,
                pays="MA",
                raw_source="hespress_rss",
            )
            results.append(article)
            time.sleep(self.delay)  # politesse

        return results

    # ------------------------------------------------------------------
    # Extraction du contenu HTML
    # ------------------------------------------------------------------

    def _fetch_full_content(self, url: str) -> Optional[str]:
        """
        Télécharge la page HTML et extrait le texte de l'article principal.
        Hespress place le contenu dans <div class="article-content">.
        """
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Sélecteurs CSS spécifiques à Hespress
            selectors = [
                "div.article-content",
                "div.single-content",
                "div.entry-content",
                "article",
            ]
            for sel in selectors:
                tag = soup.select_one(sel)
                if tag:
                    # Supprime les balises de navigation / publicité imbriquées
                    for unwanted in tag.select("aside, nav, script, style, figure"):
                        unwanted.decompose()
                    text = tag.get_text(separator="\n").strip()
                    if len(text) > 100:
                        return text

            # Fallback : tout le <body>
            body = soup.find("body")
            return body.get_text(separator="\n").strip() if body else None

        except Exception as exc:
            self.logger.debug(f"Erreur de récupération contenu ({url}): {exc}")
            return None

    # ------------------------------------------------------------------
    # Normalisation de date
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """
        Convertit les formats RSS courants vers ISO 8601 UTC.
        Exemple : 'Sat, 26 Apr 2025 10:00:00 +0000' → '2025-04-26T10:00:00'
        """
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
        return raw  # retourne brut si parsing échoue
