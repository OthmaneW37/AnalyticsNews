"""
silver_processor.py
--------------------
Traitement de la couche Bronze → Silver.

Couche Silver = données NETTOYÉES et NORMALISÉES, prêtes pour l'analyse.
- Suppression HTML
- Normalisation du texte (espaces, casse)
- Détection de la langue
- Contrôle qualité (titre vide, contenu trop court)
- Déduplication par article_id
- [Phase 3] BERTopic sera intégré ici

Dépendances : pandas, langdetect (optionnel)
"""

import hashlib
import json
import logging
import re
import os
import io
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

logger = logging.getLogger(__name__)

# Seuils de qualité
MIN_CONTENT_LENGTH = 50   # caractères minimum pour un contenu valide
MIN_TITLE_LENGTH = 5       # caractères minimum pour un titre valide

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


class SilverProcessor:
    """
    Transforme les articles Bronze en données Silver exploitables.

    Usage
    -----
    >>> processor = SilverProcessor()
    >>> silver_df = processor.process(bronze_articles)
    >>> processor.save(silver_df, source="hespress")
    """

    def __init__(self, silver_root: str = "data/silver", use_minio: bool = False):
        self.silver_root = Path(silver_root)
        self.use_minio = use_minio and MINIO_AVAILABLE
        self.bucket_name = "silver"

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
                logger.info(f"[Silver] Connecté à MinIO ({MINIO_ENDPOINT})")
            except Exception as e:
                logger.error(f"[Silver] Erreur connexion MinIO : {e}")
                self.use_minio = False

    # ==================================================================
    # POINT D'ENTRÉE PRINCIPAL
    # ==================================================================

    def process(self, bronze_articles: list[dict]) -> pd.DataFrame:
        """
        Pipeline de nettoyage complet Bronze → Silver.

        Étapes :
        1. Chargement en DataFrame
        2. Déduplication
        3. Nettoyage HTML & texte
        4. Normalisation des champs
        5. Contrôle qualité
        6. Enrichissement (langue détectée, hash contenu)

        Paramètres
        ----------
        bronze_articles : list[dict]
            Sortie directe de BronzeWriter.read_latest() ou BaseScraper.run().

        Retourne
        --------
        pd.DataFrame avec les articles nettoyés et un champ `quality_status`.
        """
        if not bronze_articles:
            logger.warning("[Silver] Aucun article à traiter.")
            return pd.DataFrame()

        df = pd.DataFrame(bronze_articles)
        logger.info(f"[Silver] {len(df)} articles reçus depuis Bronze.")

        # ------------------------------------------------------------------
        # 1. Déduplication par article_id
        # ------------------------------------------------------------------
        before = len(df)
        df = df.drop_duplicates(subset=["article_id"])
        dupes = before - len(df)
        if dupes:
            logger.info(f"[Silver] {dupes} doublons supprimés.")

        # ------------------------------------------------------------------
        # 2. Nettoyage du contenu
        # ------------------------------------------------------------------
        df["contenu_clean"] = df["contenu"].apply(self._clean_text)
        df["titre_clean"] = df["titre"].apply(self._clean_text)

        # ------------------------------------------------------------------
        # 3. Normalisation des champs
        # ------------------------------------------------------------------
        df["date_publication"] = pd.to_datetime(
            df["date_publication"], errors="coerce"
        )
        df["source"] = df["source"].str.lower().str.strip()
        df["langue"] = df["langue"].str.lower().str.strip()
        df["pays"] = df["pays"].str.upper().str.strip()
        df["auteur"] = df.get("auteur", pd.Series([""] * len(df))).astype(str).str.strip()
        df["categorie"] = df.get("categorie", pd.Series([""] * len(df))).astype(str).str.strip()

        # ------------------------------------------------------------------
        # 4. Détection automatique de la langue (cohérence)
        # ------------------------------------------------------------------
        df["langue_detectee"] = df["contenu_clean"].apply(self._detect_language)

        # ------------------------------------------------------------------
        # 5. Champs enrichis
        # ------------------------------------------------------------------
        df["content_length"] = df["contenu_clean"].str.len()
        df["content_hash"] = df["contenu_clean"].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest() if x else ""
        )
        df["processed_at"] = datetime.utcnow().isoformat()

        # ------------------------------------------------------------------
        # 6. Contrôle qualité (complétude, cohérence, validité)
        # ------------------------------------------------------------------
        df["quality_flags"] = df.apply(self._check_quality, axis=1)
        df["quality_status"] = df["quality_flags"].apply(
            lambda flags: "FAIL" if flags else "OK"
        )

        ok_count = (df["quality_status"] == "OK").sum()
        fail_count = (df["quality_status"] == "FAIL").sum()
        logger.info(
            f"[Silver] Qualité — OK: {ok_count} | FAIL: {fail_count}"
        )

        # ------------------------------------------------------------------
        # 7. Sélection & ordre des colonnes Silver
        # ------------------------------------------------------------------
        silver_cols = [
            "article_id",
            "titre_clean",
            "url",
            "source",
            "langue",
            "langue_detectee",
            "pays",
            "date_publication",
            "contenu_clean",
            "content_length",
            "content_hash",
            "raw_source",
            "auteur",
            "categorie",
            "quality_flags",
            "quality_status",
            "processed_at",
        ]
        # Garde uniquement les colonnes existantes (robustesse)
        existing_cols = [c for c in silver_cols if c in df.columns]
        return df[existing_cols].reset_index(drop=True)

    # ==================================================================
    # NETTOYAGE TEXTE
    # ==================================================================

    @staticmethod
    def _clean_text(text: str) -> str:
        """
        Nettoyage complet d'un texte :
        - Suppression des balises HTML
        - Suppression des URLs
        - Normalisation des espaces blancs
        - Suppression des caractères de contrôle
        """
        if not isinstance(text, str) or not text.strip():
            return ""

        # 1. Balises HTML
        text = re.sub(r"<[^>]+>", " ", text)

        # 2. Entités HTML (&amp; &nbsp; etc.)
        text = re.sub(r"&[a-zA-Z]{2,6};", " ", text)
        text = re.sub(r"&#\d+;", " ", text)

        # 3. URLs
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"www\.\S+", "", text)

        # 4. Caractères de contrôle (sauf \n et \t)
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

        # 5. Espaces multiples et sauts de ligne excessifs
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        return text

    # ==================================================================
    # CONTRÔLE QUALITÉ (Complétude, Cohérence, Validité)
    # ==================================================================

    def _check_quality(self, row: pd.Series) -> list[str]:
        """
        Retourne la liste des problèmes qualité détectés.
        Dimensions : Complétude, Cohérence, Validité.
        Une liste vide = article de bonne qualité.
        """
        flags = []

        # --- COMPLÉTUDE ---
        if not isinstance(row.get("titre_clean"), str) or len(row.get("titre_clean", "")) < MIN_TITLE_LENGTH:
            flags.append("TITRE_VIDE_OU_TROP_COURT")

        content = row.get("contenu_clean", "")
        if not isinstance(content, str) or len(content) < MIN_CONTENT_LENGTH:
            flags.append("CONTENU_TROP_COURT")

        if not row.get("url"):
            flags.append("URL_MANQUANTE")

        if pd.isna(row.get("date_publication")):
            flags.append("DATE_MANQUANTE")

        # Auteur et catégorie manquants = warning (ne bloque pas la qualité)
        # car de nombreux flux RSS ne fournissent pas ces champs.

        # --- VALIDITÉ ---
        url = row.get("url", "")
        if url and not self._is_url_valid(str(url)):
            flags.append("URL_INVALIDE")

        # --- COHÉRENCE (warning, non bloquant) ---
        declared_lang = str(row.get("langue", "")).lower()
        detected_lang = str(row.get("langue_detectee", "")).lower()
        content_len = len(str(row.get("contenu_clean", "")))
        if content_len >= 200 and detected_lang and declared_lang and detected_lang != declared_lang and detected_lang != "unknown":
            flags.append("LANGUE_INCOHERENTE")

        return flags

    @staticmethod
    def _is_url_valid(url: str) -> bool:
        """Vérifie que l'URL commence par http(s) et contient un domaine."""
        if not url:
            return False
        return bool(re.match(r"^https?://[^\s/$.?#].[^\s]*$", url, re.IGNORECASE))

    @staticmethod
    def _detect_language(text: str) -> str:
        """
        Détecte la langue d'un texte avec langdetect.
        Retourne le code ISO 639-1 (fr, en, ar, etc.) ou 'unknown'.
        """
        if not isinstance(text, str) or len(text.strip()) < 20:
            return "unknown"
        try:
            from langdetect import detect
            lang = detect(text)
            return lang
        except Exception:
            return "unknown"

    # ==================================================================
    # PERSISTANCE SILVER
    # ==================================================================

    def save(self, df: pd.DataFrame, source: str) -> Path | str:
        """
        Sauvegarde le DataFrame Silver en Parquet et JSON.
        Parquet = format colonnaire efficace pour l'analyse.
        JSON    = lisibilité humaine pour debug.
        """
        if df is None or df.empty:
            logger.warning(f"[Silver] Rien à sauvegarder pour '{source}'.")
            return None

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        # Parquet (format analytique)
        parquet_filename = f"{source}_{date_str}_{time_str}.parquet"
        # JSON (debug)
        json_filename = f"{source}_{date_str}_{time_str}.json"

        # Écriture sur MinIO
        if self.use_minio:
            parquet_obj_name = f"{source}/{date_str}/{parquet_filename}"
            json_obj_name = f"{source}/{date_str}/{json_filename}"
            
            # Sauvegarde mémoire puis MinIO
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
            parquet_bytes = parquet_buffer.getvalue()
            
            json_buffer = io.BytesIO()
            records = df.copy()
            records["date_publication"] = records["date_publication"].astype(str)
            records.to_json(json_buffer, orient="records", force_ascii=False, indent=2)
            json_bytes = json_buffer.getvalue()

            try:
                self.s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=parquet_obj_name,
                    data=io.BytesIO(parquet_bytes),
                    length=len(parquet_bytes),
                    content_type="application/vnd.apache.parquet"
                )
                logger.info(f"[Silver] Parquet MinIO → s3://{self.bucket_name}/{parquet_obj_name}")
                
                self.s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=json_obj_name,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type="application/json"
                )
                logger.info(f"[Silver] JSON MinIO → s3://{self.bucket_name}/{json_obj_name}")
                return f"s3://{self.bucket_name}/{parquet_obj_name}"
            except Exception as e:
                logger.error(f"[Silver] Erreur MinIO : {e}")
                logger.info("[Silver] Fallback vers l'écriture locale.")

        # Écriture Locale
        partition_dir = self.silver_root / source / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = partition_dir / parquet_filename
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"[Silver] Parquet sauvegardé → {parquet_path}")

        json_path = partition_dir / json_filename
        records = df.copy()
        records["date_publication"] = records["date_publication"].astype(str)
        records.to_json(json_path, orient="records", force_ascii=False, indent=2)
        logger.info(f"[Silver] JSON sauvegardé → {json_path}")

        return parquet_path

    def load(self, source: str, date: str = None) -> pd.DataFrame:
        """Charge le fichier Silver Parquet le plus récent."""
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        
        if self.use_minio:
            prefix = f"{source}/{date}/"
            try:
                objects = list(self.s3_client.list_objects(self.bucket_name, prefix=prefix))
                parquet_objects = [obj for obj in objects if obj.object_name.endswith('.parquet')]
                if not parquet_objects:
                    logger.warning(f"[Silver] Aucune partition MinIO trouvée : {prefix}")
                    return pd.DataFrame()
                
                latest_obj = sorted(parquet_objects, key=lambda x: x.object_name, reverse=True)[0]
                response = self.s3_client.get_object(self.bucket_name, latest_obj.object_name)
                df = pd.read_parquet(io.BytesIO(response.read()))
                response.close()
                response.release_conn()
                
                logger.info(f"[Silver] Chargé {len(df)} articles depuis MinIO {latest_obj.object_name}")
                return df
            except Exception as e:
                logger.error(f"[Silver] Erreur lecture MinIO : {e}")
                return pd.DataFrame()

        # Local
        partition_dir = self.silver_root / source / date

        if not partition_dir.exists():
            logger.warning(f"[Silver] Partition introuvable : {partition_dir}")
            return pd.DataFrame()

        parquet_files = sorted(partition_dir.glob("*.parquet"), reverse=True)
        if not parquet_files:
            return pd.DataFrame()

        df = pd.read_parquet(parquet_files[0])
        logger.info(f"[Silver] Chargé {len(df)} articles depuis {parquet_files[0]}")
        return df

    # ==================================================================
    # DIFFÉRENCIATEUR #2 : BERTopic — Topic Modeling Multilingue
    # ==================================================================

    def apply_bertopic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applique BERTopic sur la colonne 'contenu_clean' pour détecter
        automatiquement les sujets dominants dans le corpus.

        Nécessite : pip install bertopic sentence-transformers torch

        Paramètres
        ----------
        df : pd.DataFrame
            DataFrame Silver avec colonne 'contenu_clean'.

        Retourne
        --------
        pd.DataFrame enrichi avec colonnes topic_id, topic_label, topic_prob.
        """
        try:
            from bertopic import BERTopic
        except ImportError:
            logger.warning(
                "[BERTopic] bertopic non installé. Colonnes topic_* remplies avec des valeurs par défaut.\n"
                "  → Installez avec : pip install bertopic sentence-transformers torch"
            )
            df = df.copy()
            df["topic_id"] = -1
            df["topic_label"] = "non-modélisé"
            df["topic_prob"] = 0.0
            return df

        if df.empty or "contenu_clean" not in df.columns:
            return df

        df = df.copy()
        docs = df["contenu_clean"].fillna("").tolist()

        # On retire les docs trop courts (BERTopic ne peut pas les traiter)
        valid_mask = df["contenu_clean"].str.len() >= MIN_CONTENT_LENGTH
        valid_docs = df.loc[valid_mask, "contenu_clean"].fillna("").tolist()

        if len(valid_docs) < 5:
            logger.warning(f"[BERTopic] Seulement {len(valid_docs)} docs valides — topic modeling ignoré.")
            df["topic_id"] = -1
            df["topic_label"] = "hors-sujet"
            df["topic_prob"] = 0.0
            return df

        logger.info(f"[BERTopic] Entraînement sur {len(valid_docs)} documents...")

        topic_model = BERTopic(
            language="multilingual",   # gère FR, AR, EN simultanément
            nr_topics="auto",          # détecte automatiquement le bon nombre
            min_topic_size=max(3, len(valid_docs) // 20),  # au moins 5% du corpus
            verbose=False,
        )

        topics, probs = topic_model.fit_transform(valid_docs)

        # Nom du topic (ex: '0_gaza_israel_guerre_ceasefire')
        topic_info = topic_model.get_topic_info()
        topic_name_map = dict(zip(topic_info["Topic"], topic_info["Name"]))

        # Remplissage du DataFrame : docs invalides → topic -1
        all_topics = []
        all_probs = []
        all_labels = []
        valid_idx = 0

        for i, row in df.iterrows():
            if valid_mask.loc[i]:
                t = topics[valid_idx]
                p = float(probs[valid_idx].max()) if hasattr(probs[valid_idx], 'max') else float(probs[valid_idx])
                l = topic_name_map.get(t, "hors-sujet") if t != -1 else "hors-sujet"
                valid_idx += 1
            else:
                t, p, l = -1, 0.0, "hors-sujet"

            all_topics.append(t)
            all_probs.append(round(p, 4))
            all_labels.append(l)

        df["topic_id"] = all_topics
        df["topic_label"] = all_labels
        df["topic_prob"] = all_probs

        n_topics = len(set(t for t in all_topics if t >= 0))
        logger.info(f"[BERTopic] {n_topics} topics détectés sur {len(valid_docs)} documents.")

        return df

