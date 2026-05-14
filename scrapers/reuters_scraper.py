"""
reuters_scraper.py
------------------
Scraper pour Reuters (https://www.reuters.com) — média international.
Stratégie : lecture des flux RSS par section.
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

REUTERS_FEEDS = {
    "world": "https://www.reutersagency.com/feed/?best-topics=political-general&post_type=reuters-best",
    "business": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=reuters-best",
    "tech": "https://www.reutersagency.com/feed/?best-topics=tech&post_type=reuters-best",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NewsPipelineBot/1.0; "
        "+https://github.com/your-org/news-pipeline)"
    )
}


class ReutersScraper(BaseScraper):
    """
    Scrape les articles Reuters via RSS.
    """

    def __init__(
        self,
        categories: Optional[list[str]] = None,
        max_per_feed: int = 25,
        fetch_content: bool = True,
        delay: float = 0.5,
    ):
        super().__init__(name="reuters")
        self.categories = categories or list(REUTERS_FEEDS.keys())
        self.max_per_feed = max_per_feed
        self.fetch_content = fetch_content
        self.delay = delay

    def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []
        seen_urls: set[str] = set()

        for cat in self.categories:
            feed_url = REUTERS_FEEDS.get(cat)
            if not feed_url:
                self.logger.warning(f"Catégorie inconnue : {cat}")
                continue
            self.logger.info(f"Lecture du flux Reuters [{cat}] : {feed_url}")
            results = self._parse_feed(feed_url, cat, seen_urls)
            articles.extend(results)

        return articles

    def _parse_feed(self, feed_url: str, category: str, seen_urls: set) -> list[Article]:
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
            date_raw = entry.get("published") or entry.get("updated") or ""
            date_pub = self._normalize_date(date_raw)
            summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(separator=" ").strip()
            auteur = entry.get("author", "").strip()
            categories = [t.get("term", "") for t in entry.get("tags", [])]
            categorie = categories[0] if categories else category

            contenu = summary
            if self.fetch_content:
                full = self._fetch_full_content(url)
                contenu = full if full else summary

            article = Article(
                titre=titre,
                url=url,
                source="reuters.com",
                langue="en",
                date_publication=date_pub,
                contenu=contenu,
                pays="US",
                raw_source=f"reuters_rss_{category}",
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
                "article",
                "div.ArticleBody__container",
                "div.StandardArticleBody_body",
                "div#___main",
            ]
            for sel in selectors:
                tag = soup.select_one(sel)
                if tag:
                    for unwanted in tag.select("aside, nav, script, style, figure"):
                        unwanted.decompose()
                    text = tag.get_text(separator="\n").strip()
                    if len(text) > 100:
                        return text

            return None
        except Exception as exc:
            self.logger.debug(f"Erreur récupération contenu ({url}): {exc}")
            return None

    @staticmethod
    def _normalize_date(raw: str) -> str:
        if not raw:
            return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
