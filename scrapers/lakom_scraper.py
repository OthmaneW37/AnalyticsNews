"""
lakom_scraper.py
----------------
Scraper pour Lakom (https://www.lakome2.com) — média marocain.
Stratégie : lecture du flux RSS principal.
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

LAKOM_FEEDS = [
    "https://lakome2.com/feed",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


class LakomScraper(BaseScraper):
    """
    Scrape les articles Lakom via RSS.
    """

    def __init__(
        self,
        max_per_feed: int = 20,
        fetch_content: bool = True,
        delay: float = 1.0,
    ):
        super().__init__(name="lakom")
        self.max_per_feed = max_per_feed
        self.fetch_content = fetch_content
        self.delay = delay

    def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []
        seen_urls: set[str] = set()

        for feed_url in LAKOM_FEEDS:
            self.logger.info(f"Lecture du flux : {feed_url}")
            feed_articles = self._parse_feed(feed_url, seen_urls)
            articles.extend(feed_articles)

        return articles

    def _parse_feed(self, feed_url: str, seen_urls: set) -> list[Article]:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as exc:
            self.logger.warning(f"Impossible de lire {feed_url} : {exc}")
            return []

        results = []
        for entry in feed.entries[:self.max_per_feed]:
            url = entry.get("link", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            titre = entry.get("title", "").strip()
            date_raw = entry.get("published", "")
            date_pub = self._normalize_date(date_raw)
            summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(separator=" ").strip()
            auteur = entry.get("author", "").strip()
            categories = [t.get("term", "") for t in entry.get("tags", [])]
            categorie = categories[0] if categories else ""

            contenu = summary
            if self.fetch_content:
                full = self._fetch_full_content(url)
                contenu = full if full else summary

            article = Article(
                titre=titre,
                url=url,
                source="lakome2.com",
                langue="fr",
                date_publication=date_pub,
                contenu=contenu,
                pays="MA",
                raw_source="lakom_rss",
                auteur=auteur,
                categorie=categorie,
            )
            results.append(article)
            time.sleep(self.delay)

        return results

    def _fetch_full_content(self, url: str) -> Optional[str]:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            selectors = [
                "div.entry-content",
                "div.post-content",
                "article",
                "div.content",
            ]
            for sel in selectors:
                tag = soup.select_one(sel)
                if tag:
                    for unwanted in tag.select("aside, nav, script, style, figure"):
                        unwanted.decompose()
                    text = tag.get_text(separator="\n").strip()
                    if len(text) > 100:
                        return text

            body = soup.find("body")
            return body.get_text(separator="\n").strip() if body else None
        except Exception as exc:
            self.logger.debug(f"Erreur récupération contenu ({url}): {exc}")
            return None

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
