"""
bronze_writer.py
----------------
Écrit les articles bruts dans la couche Bronze du Data Lake.

Couche Bronze = données brutes, SANS transformation, en JSON.
Chaque session de scraping crée un fichier partitionné par date/source.

Structure Bronze :
  bronze/
    hespress/
      2025-04-26/
        hespress_2025-04-26_143022.json
    bbc/
      2025-04-26/
        bbc_2025-04-26_143105.json
    gdelt/
      2025-04-26/
        gdelt_2025-04-26_143200.json
"""

import json
import logging
import os
import io
from datetime import datetime
from pathlib import Path

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

logger = logging.getLogger(__name__)

# Chemin racine du Data Lake local (modifiable via variable d'env)
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", "data/bronze"))

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


class BronzeWriter:
    """
    Persistance des articles bruts en JSON sur le système de fichiers local
    ou sur MinIO (S3) si configuré.

    Paramètres
    ----------
    root : Path | str
        Répertoire racine de la couche Bronze (local).
    use_minio : bool
        Si True, écrit dans le bucket 'bronze' sur MinIO.
    """

    def __init__(self, root: Path | str = BRONZE_ROOT, use_minio: bool = False):
        self.root = Path(root)
        self.use_minio = use_minio and MINIO_AVAILABLE
        self.bucket_name = "bronze"

        if self.use_minio:
            try:
                self.s3_client = Minio(
                    MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=MINIO_SECURE,
                )
                if not self.s3_client.bucket_exists(self.bucket_name):
                    self.s3_client.make_bucket(self.bucket_name)
                logger.info(f"[Bronze] Connecté à MinIO ({MINIO_ENDPOINT})")
            except Exception as e:
                logger.error(f"[Bronze] Erreur connexion MinIO : {e}")
                self.use_minio = False

    def write(self, source: str, articles: list[dict]) -> Path | str:
        """
        Écrit une liste d'articles dans un fichier JSON partitionné.
        Sauvegarde sur MinIO si use_minio=True, sinon en local.

        Paramètres
        ----------
        source : str
            Nom de la source (ex: "hespress", "bbc", "gdelt").
        articles : list[dict]
            Articles au format dict (sortie de BaseScraper.run()).

        Retourne
        --------
        Path | str : chemin local ou URI MinIO du fichier créé.
        """
        if not articles:
            logger.warning(f"[Bronze] Aucun article à écrire pour '{source}'.")
            return None

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        filename = f"{source}_{date_str}_{time_str}.json"
        
        payload = {
            "metadata": {
                "source": source,
                "written_at": now.isoformat(),
                "article_count": len(articles),
                "schema_version": "1.0",
            },
            "articles": articles,
        }

        # Écriture sur MinIO
        if self.use_minio:
            object_name = f"{source}/{date_str}/{filename}"
            json_data = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
            json_stream = io.BytesIO(json_data)
            try:
                self.s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    data=json_stream,
                    length=len(json_data),
                    content_type="application/json"
                )
                logger.info(f"[Bronze] {len(articles)} articles écrits sur MinIO → s3://{self.bucket_name}/{object_name}")
                return f"s3://{self.bucket_name}/{object_name}"
            except Exception as e:
                logger.error(f"[Bronze] Erreur d'écriture MinIO : {e}")
                # Fallback en local en cas d'erreur
                logger.info("[Bronze] Fallback vers l'écriture locale.")

        # Écriture Locale (ou Fallback)
        partition_dir = self.root / source / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)
        filepath = partition_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(f"[Bronze] {len(articles)} articles écrits en local → {filepath}")
        return filepath

    # ------------------------------------------------------------------
    # Lecture (utile pour les tests et Silver)
    # ------------------------------------------------------------------

    def read_latest(self, source: str, date: str = None) -> list[dict]:
        """
        Lit le fichier Bronze le plus récent pour une source donnée.
        """
        date = date or datetime.utcnow().strftime("%Y-%m-%d")

        if self.use_minio:
            prefix = f"{source}/{date}/"
            try:
                objects = list(self.s3_client.list_objects(self.bucket_name, prefix=prefix))
                if not objects:
                    logger.warning(f"[Bronze] Aucune partition MinIO trouvée : {prefix}")
                    return []
                
                # Le plus récent = dernier par ordre alphabétique car on a YYYY-MM-DD_HHMMSS
                latest_obj = sorted(objects, key=lambda x: x.object_name, reverse=True)[0]
                
                response = self.s3_client.get_object(self.bucket_name, latest_obj.object_name)
                payload = json.loads(response.read().decode('utf-8'))
                response.close()
                response.release_conn()
                
                logger.info(f"[Bronze] Lecture MinIO {latest_obj.object_name} — {len(payload['articles'])} articles.")
                return payload.get("articles", [])
            except Exception as e:
                logger.error(f"[Bronze] Erreur lecture MinIO : {e}")
                return []

        # Local
        partition_dir = self.root / source / date
        if not partition_dir.exists():
            logger.warning(f"[Bronze] Aucune partition trouvée : {partition_dir}")
            return []

        json_files = sorted(partition_dir.glob("*.json"), reverse=True)
        if not json_files:
            return []

        latest = json_files[0]
        with open(latest, "r", encoding="utf-8") as f:
            payload = json.load(f)

        logger.info(f"[Bronze] Lecture de {latest} — {len(payload['articles'])} articles.")
        return payload.get("articles", [])

    def read_all(self, source: str, date: str = None) -> list[dict]:
        """
        Lit TOUS les fichiers Bronze d'une source pour une date donnée.
        """
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        all_articles = []

        if self.use_minio:
            prefix = f"{source}/{date}/"
            try:
                objects = list(self.s3_client.list_objects(self.bucket_name, prefix=prefix))
                for obj in objects:
                    response = self.s3_client.get_object(self.bucket_name, obj.object_name)
                    payload = json.loads(response.read().decode('utf-8'))
                    response.close()
                    response.release_conn()
                    all_articles.extend(payload.get("articles", []))
                
                logger.info(f"[Bronze] {len(all_articles)} articles totaux MinIO pour {source}/{date}")
                return all_articles
            except Exception as e:
                logger.error(f"[Bronze] Erreur lecture totale MinIO : {e}")
                return []

        # Local
        partition_dir = self.root / source / date
        if not partition_dir.exists():
            return []

        for json_file in sorted(partition_dir.glob("*.json")):
            with open(json_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            all_articles.extend(payload.get("articles", []))

        logger.info(f"[Bronze] {len(all_articles)} articles totaux pour {source}/{date}")
        return all_articles
