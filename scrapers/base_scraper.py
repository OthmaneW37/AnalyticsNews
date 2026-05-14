"""
base_scraper.py
---------------
Classe abstraite dont héritent tous les scrapers.
Définit le contrat : chaque scraper DOIT implémenter `fetch_articles()`.
"""

import abc
import hashlib
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Article:
    """Structure de données canonique pour un article brut (Bronze)."""

    def __init__(
        self,
        titre: str,
        url: str,
        source: str,
        langue: str,
        date_publication: Optional[str] = None,
        contenu: Optional[str] = None,
        pays: Optional[str] = None,
        raw_source: Optional[str] = None,
        auteur: Optional[str] = None,
        categorie: Optional[str] = None,
    ):
        self.titre = titre
        self.url = url
        self.source = source
        self.langue = langue
        self.date_publication = date_publication or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        self.contenu = contenu or ""
        self.pays = pays or "unknown"
        self.raw_source = raw_source or source
        self.auteur = auteur or ""
        self.categorie = categorie or ""
        # Identifiant déterministe basé sur l'URL
        self.article_id = hashlib.md5(url.encode()).hexdigest()

    def to_dict(self) -> dict:
        return {
            "article_id": self.article_id,
            "titre": self.titre,
            "url": self.url,
            "source": self.source,
            "langue": self.langue,
            "date_publication": self.date_publication,
            "contenu": self.contenu,
            "pays": self.pays,
            "raw_source": self.raw_source,
            "auteur": self.auteur,
            "categorie": self.categorie,
            "ingested_at": datetime.utcnow().isoformat(),
        }


class BaseScraper(abc.ABC):
    """
    Classe de base pour tous les scrapers du pipeline.
    Chaque scraper hérite de cette classe et surcharge `fetch_articles`.
    """

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"scraper.{name}")

    @abc.abstractmethod
    def fetch_articles(self) -> list[Article]:
        """
        Récupère une liste d'articles depuis la source.
        Doit retourner une liste d'objets `Article`.
        """
        ...

    def run(self) -> list[dict]:
        """
        Point d'entrée standardisé — appelé par le pipeline batch.
        Retourne une liste de dicts prêts pour la sérialisation JSON.
        """
        self.logger.info(f"[{self.name}] Démarrage du scraping...")
        try:
            articles = self.fetch_articles()
            self.logger.info(f"[{self.name}] {len(articles)} articles récupérés.")
            return [a.to_dict() for a in articles]
        except Exception as exc:
            self.logger.error(f"[{self.name}] Erreur lors du scraping : {exc}")
            return []
