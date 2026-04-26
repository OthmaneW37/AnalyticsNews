"""
gdelt_client.py
---------------
Client pour l'API GDELT v2 — Différenciateur #1.
Récupère des articles réels de médias mondiaux via une API REST gratuite.

Dépendances : requests, pandas
"""

import logging
from datetime import datetime

import requests
import pandas as pd

from scrapers.base_scraper import Article, BaseScraper

logger = logging.getLogger(__name__)


class GDELTClient(BaseScraper):
    """
    Interroge l'API GDELT v2 DOC pour récupérer des articles
    filtrés par requête textuelle, langue, et fenêtre temporelle.

    Paramètres
    ----------
    query : str
        Requête de recherche (ex: "Maroc économie", "Gaza", "football").
    max_records : int
        Nombre maximum d'articles (max GDELT = 250).
    timespan : str
        Fenêtre temporelle GDELT : "1h", "6h", "1d", "1w", "1m".
    sourcelang : str
        Langue source : "french", "english", "arabic".
    """

    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(
        self,
        query: str = "Morocco",
        max_records: int = 250,
        timespan: str = "6h",
        sourcelang: str = "french",
    ):
        super().__init__(name="gdelt")
        self.query = query
        self.max_records = max_records
        self.timespan = timespan
        self.sourcelang = sourcelang

    # ------------------------------------------------------------------
    # Méthode principale
    # ------------------------------------------------------------------

    def fetch_articles(self) -> list[Article]:
        raw_articles = self._call_api()
        return [self._to_article(a) for a in raw_articles]

    # ------------------------------------------------------------------
    # Appel API GDELT v2
    # ------------------------------------------------------------------

    def _call_api(self) -> list[dict]:
        params = {
            "query": self.query,
            "mode": "ArtList",
            "maxrecords": self.max_records,
            "format": "json",
            "timespan": self.timespan,
            "sourcelang": self.sourcelang,
        }
        self.logger.info(
            f"GDELT query='{self.query}' timespan={self.timespan} lang={self.sourcelang}"
        )
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            self.logger.info(f"GDELT : {len(articles)} articles reçus.")
            return articles
        except Exception as exc:
            self.logger.error(f"GDELT API error : {exc}")
            return []

    # ------------------------------------------------------------------
    # Conversion vers Article canonique
    # ------------------------------------------------------------------

    def _to_article(self, a: dict) -> Article:
        raw_date = a.get("seendate", "")
        # Format GDELT : '20250426T100000Z'
        date_pub = self._parse_gdelt_date(raw_date)

        return Article(
            titre=a.get("title", ""),
            url=a.get("url", ""),
            source=a.get("domain", "unknown"),
            langue=a.get("language", "unknown"),
            date_publication=date_pub,
            contenu=a.get("title", ""),   # GDELT ne fournit que le titre
            pays=a.get("sourcecountry", "unknown"),
            raw_source="gdelt",
        )

    # ------------------------------------------------------------------
    # Parsing date GDELT
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_gdelt_date(raw: str) -> str:
        """Convertit '20250426T100000Z' → '2025-04-26T10:00:00'."""
        if not raw:
            return datetime.utcnow().isoformat()
        try:
            dt = datetime.strptime(raw, "%Y%m%dT%H%M%SZ")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return raw

    # ------------------------------------------------------------------
    # Utilitaire : conversion vers DataFrame (pour debug/exploration)
    # ------------------------------------------------------------------

    def fetch_as_dataframe(self) -> pd.DataFrame:
        """
        Retourne un DataFrame pandas des articles GDELT.
        Pratique pour l'exploration en notebook.
        """
        articles = self._call_api()
        if not articles:
            return pd.DataFrame()
        return pd.DataFrame(articles)
