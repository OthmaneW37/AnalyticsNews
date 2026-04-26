"""
gold_aggregator.py
------------------
Couche Gold : agrégation finale et enrichissement avec signaux marché.

Pipeline Gold :
  Silver DataFrame
    → Agrégation par topic (BERTopic)
    → Enrichissement Polymarket (Différenciateur #3)
    → Sauvegarde Gold (Parquet + JSON)

Structure Gold :
  gold/
    2026-04-26/
      gold_2026-04-26_143022.parquet   ← articles enrichis
      topics_2026-04-26_143022.json    ← résumé des topics
"""

import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


class PolymarketEnricher:
    """
    Différenciateur #3 : Enrichissement avec les marchés prédictifs Polymarket.

    Pour chaque topic BERTopic détecté dans les articles, cherche un marché
    Polymarket correspondant et récupère la probabilité de marché courante.

    Cela transforme nos données éditoriales en données de "signal" :
    → 68% de probabilité que X se produise d'après les marchés.
    """

    BASE_URL = "https://gamma-api.polymarket.com/markets"

    def __init__(self, timeout: int = 8):
        self.timeout = timeout

    def fetch_market_signals(self, keywords: list[str]) -> dict:
        """
        Pour chaque mot-clé (label de topic), cherche un marché Polymarket
        actif correspondant et récupère la probabilité.

        Paramètres
        ----------
        keywords : list[str]
            Labels de topics BERTopic (ex: ['israel_gaza', 'maroc_economie']).

        Retourne
        --------
        dict : {keyword: {market_question, probability, volume_usd, url}}
        """
        signals = {}

        for keyword in keywords:
            if not keyword or keyword == "hors-sujet" or keyword.startswith("-1"):
                continue

            # On nettoie le label BERTopic (ex: '0_gaza_israel_guerre' → 'gaza israel')
            search_term = " ".join(keyword.replace("_", " ").split()[1:4])
            if not search_term.strip():
                continue

            try:
                resp = requests.get(
                    self.BASE_URL,
                    params={
                        "search": search_term,
                        "limit": 1,
                        "active": "true",
                        "closed": "false",
                    },
                    timeout=self.timeout,
                )

                if resp.ok and resp.json():
                    market = resp.json()[0]
                    outcome_prices = market.get("outcomePrices", "[]")

                    # outcomePrices peut être une chaîne JSON ou une liste
                    if isinstance(outcome_prices, str):
                        import ast
                        try:
                            outcome_prices = ast.literal_eval(outcome_prices)
                        except Exception:
                            outcome_prices = [0.5]

                    probability = float(outcome_prices[0]) if outcome_prices else 0.5

                    signals[keyword] = {
                        "search_term": search_term,
                        "market_question": market.get("question", ""),
                        "probability": round(probability, 4),
                        "probability_pct": f"{probability * 100:.1f}%",
                        "volume_usd": market.get("volume", 0),
                        "url": market.get("url", ""),
                        "fetched_at": datetime.utcnow().isoformat(),
                    }
                    logger.info(
                        f"[Polymarket] '{search_term}' → "
                        f"{signals[keyword]['probability_pct']} ({signals[keyword]['market_question'][:60]})"
                    )
                else:
                    logger.debug(f"[Polymarket] Aucun marché trouvé pour : '{search_term}'")

            except requests.exceptions.Timeout:
                logger.warning(f"[Polymarket] Timeout pour '{search_term}'")
            except Exception as exc:
                logger.error(f"[Polymarket] Erreur pour '{search_term}' : {exc}")

        logger.info(f"[Polymarket] {len(signals)} signaux récupérés sur {len(keywords)} topics.")
        return signals

    def enrich_dataframe(self, df: pd.DataFrame, topic_signals: dict) -> pd.DataFrame:
        """
        Ajoute les colonnes Polymarket au DataFrame Silver/Gold.

        Paramètres
        ----------
        df : pd.DataFrame
            DataFrame avec au minimum une colonne 'topic_label'.
        topic_signals : dict
            Sortie de fetch_market_signals().

        Retourne
        --------
        pd.DataFrame enrichi avec colonnes polymarket_*.
        """
        if df.empty or "topic_label" not in df.columns:
            return df

        df = df.copy()
        df["polymarket_question"] = df["topic_label"].map(
            lambda t: topic_signals.get(t, {}).get("market_question", "")
        )
        df["polymarket_prob"] = df["topic_label"].map(
            lambda t: topic_signals.get(t, {}).get("probability", None)
        )
        df["polymarket_prob_pct"] = df["topic_label"].map(
            lambda t: topic_signals.get(t, {}).get("probability_pct", "")
        )
        df["polymarket_volume_usd"] = df["topic_label"].map(
            lambda t: topic_signals.get(t, {}).get("volume_usd", None)
        )
        df["polymarket_url"] = df["topic_label"].map(
            lambda t: topic_signals.get(t, {}).get("url", "")
        )

        enriched = df["polymarket_prob"].notna().sum()
        logger.info(f"[Polymarket] {enriched}/{len(df)} articles enrichis avec des signaux marché.")
        return df


class GoldAggregator:
    """
    Orchestre la construction de la couche Gold :
    1. Charge le Silver
    2. Calcule les agrégats (stats par topic, par source, par jour)
    3. Enrichit avec Polymarket
    4. Sauvegarde en Gold

    Paramètres
    ----------
    gold_root : str
        Répertoire racine Gold (local).
    use_minio : bool
        Si True, écrit dans le bucket 'gold' sur MinIO.
    """

    def __init__(self, gold_root: str = "data/gold", use_minio: bool = False):
        self.gold_root = Path(gold_root)
        self.use_minio = use_minio and MINIO_AVAILABLE
        self.bucket_name = "gold"
        self.polymarket = PolymarketEnricher()

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
                logger.info(f"[Gold] Connecté à MinIO ({MINIO_ENDPOINT})")
            except Exception as e:
                logger.error(f"[Gold] Erreur connexion MinIO : {e}")
                self.use_minio = False

    def build_gold(self, silver_df: pd.DataFrame, enrich_polymarket: bool = True) -> pd.DataFrame:
        """
        Construit la couche Gold depuis un DataFrame Silver (déjà enrichi BERTopic).

        Étapes :
        1. Filtre les articles de qualité OK
        2. Enrichit avec Polymarket si demandé
        3. Calcule les colonnes Gold (score de couverture, signal combiné)

        Paramètres
        ----------
        silver_df : pd.DataFrame
            DataFrame Silver avec colonnes BERTopic (topic_id, topic_label, topic_prob).
        enrich_polymarket : bool
            Si True, appelle l'API Polymarket (nécessite une connexion internet).
        """
        if silver_df.empty:
            logger.warning("[Gold] DataFrame Silver vide — rien à aggréger.")
            return pd.DataFrame()

        df = silver_df.copy()

        # 1. Filtre qualité
        if "quality_status" in df.columns:
            before = len(df)
            df = df[df["quality_status"] == "OK"].copy()
            logger.info(f"[Gold] Filtre qualité : {len(df)}/{before} articles retenus.")

        # 2. Enrichissement Polymarket
        if enrich_polymarket and "topic_label" in df.columns:
            topic_labels = df["topic_label"].dropna().unique().tolist()
            signals = self.polymarket.fetch_market_signals(topic_labels)
            df = self.polymarket.enrich_dataframe(df, signals)

        # 3. Score de couverture (nb d'articles par topic normalisé)
        if "topic_label" in df.columns:
            topic_counts = df["topic_label"].value_counts()
            df["topic_article_count"] = df["topic_label"].map(topic_counts)
            max_count = df["topic_article_count"].max() if len(df) > 0 else 1
            df["topic_coverage_score"] = (df["topic_article_count"] / max_count).round(4)

        # 4. Signal combiné (prob Polymarket × topic_prob BERTopic)
        if "polymarket_prob" in df.columns and "topic_prob" in df.columns:
            df["combined_signal"] = (
                df["polymarket_prob"].fillna(0.5) * df["topic_prob"].fillna(0.5)
            ).round(4)

        df["gold_built_at"] = datetime.utcnow().isoformat()
        logger.info(f"[Gold] DataFrame Gold construit — {len(df)} articles.")
        return df

    def get_topic_summary(self, gold_df: pd.DataFrame) -> list[dict]:
        """
        Génère un résumé JSON des topics (utile pour le dashboard et le warehouse).

        Retourne une liste de dicts, un par topic, avec :
        - label, article_count, top_sources, avg_polymarket_prob
        """
        if gold_df.empty or "topic_label" not in gold_df.columns:
            return []

        summaries = []
        for topic_label, group in gold_df.groupby("topic_label"):
            summary = {
                "topic_label": topic_label,
                "article_count": len(group),
                "sources": group["source"].value_counts().to_dict() if "source" in group else {},
                "avg_polymarket_prob": (
                    round(group["polymarket_prob"].dropna().mean(), 4)
                    if "polymarket_prob" in group.columns else None
                ),
                "polymarket_question": (
                    group["polymarket_question"].dropna().iloc[0]
                    if "polymarket_question" in group.columns and not group["polymarket_question"].dropna().empty
                    else ""
                ),
                "date_range": {
                    "min": str(group["date_publication"].min()) if "date_publication" in group.columns else "",
                    "max": str(group["date_publication"].max()) if "date_publication" in group.columns else "",
                },
            }
            summaries.append(summary)

        # Trie par nombre d'articles décroissant
        summaries.sort(key=lambda x: x["article_count"], reverse=True)
        return summaries

    def save(self, gold_df: pd.DataFrame, topic_summaries: list[dict] = None) -> Path | str:
        """Sauvegarde la couche Gold en local ou sur MinIO."""
        if gold_df is None or gold_df.empty:
            logger.warning("[Gold] Rien à sauvegarder.")
            return None

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        parquet_filename = f"gold_{date_str}_{time_str}.parquet"
        topics_filename = f"topics_{date_str}_{time_str}.json"

        # Prépare les données
        gold_df_save = gold_df.copy()
        for col in gold_df_save.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns:
            gold_df_save[col] = gold_df_save[col].astype(str)

        parquet_buffer = io.BytesIO()
        gold_df_save.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_bytes = parquet_buffer.getvalue()

        topics_json = json.dumps(
            topic_summaries or [], ensure_ascii=False, indent=2
        ).encode("utf-8")

        # MinIO
        if self.use_minio:
            try:
                self.s3_client.put_object(
                    self.bucket_name, f"{date_str}/{parquet_filename}",
                    io.BytesIO(parquet_bytes), len(parquet_bytes),
                    content_type="application/vnd.apache.parquet"
                )
                self.s3_client.put_object(
                    self.bucket_name, f"{date_str}/{topics_filename}",
                    io.BytesIO(topics_json), len(topics_json),
                    content_type="application/json"
                )
                logger.info(f"[Gold] Sauvegardé sur MinIO → s3://gold/{date_str}/")
                return f"s3://gold/{date_str}/{parquet_filename}"
            except Exception as e:
                logger.error(f"[Gold] Erreur MinIO : {e}")

        # Local
        partition_dir = self.gold_root / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = partition_dir / parquet_filename
        with open(parquet_path, "wb") as f:
            f.write(parquet_bytes)

        topics_path = partition_dir / topics_filename
        with open(topics_path, "wb") as f:
            f.write(topics_json)

        logger.info(f"[Gold] Sauvegardé en local → {parquet_path}")
        return parquet_path

    def load(self, date: str = None) -> pd.DataFrame:
        """Charge le fichier Gold le plus récent."""
        date = date or datetime.utcnow().strftime("%Y-%m-%d")

        if self.use_minio:
            prefix = f"{date}/"
            try:
                objects = [
                    obj for obj in self.s3_client.list_objects(self.bucket_name, prefix=prefix)
                    if obj.object_name.endswith(".parquet")
                ]
                if not objects:
                    return pd.DataFrame()
                latest = sorted(objects, key=lambda x: x.object_name, reverse=True)[0]
                response = self.s3_client.get_object(self.bucket_name, latest.object_name)
                df = pd.read_parquet(io.BytesIO(response.read()))
                response.close()
                response.release_conn()
                return df
            except Exception as e:
                logger.error(f"[Gold] Erreur lecture MinIO : {e}")
                return pd.DataFrame()

        partition_dir = self.gold_root / date
        if not partition_dir.exists():
            return pd.DataFrame()
        parquets = sorted(partition_dir.glob("gold_*.parquet"), reverse=True)
        if not parquets:
            return pd.DataFrame()
        return pd.read_parquet(parquets[0])
