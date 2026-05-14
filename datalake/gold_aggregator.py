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
import sys
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

# Ajoute la racine au path pour importer warehouse
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from warehouse.duckdb_manager import DuckDBManager
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


class PolymarketEnricher:
    """
    Différenciateur #3 : Enrichissement avec les marchés prédictifs Polymarket.

    Stratégie : charge TOUS les événements actifs depuis l'API Gamma,
    puis fait du matching local par mots-clés. Le search de l'API Gamma
    étant peu fiable, le matching local donne de bien meilleurs résultats.
    """

    EVENTS_URL = "https://gamma-api.polymarket.com/events"
    MARKETS_URL = "https://gamma-api.polymarket.com/markets"

    # Mapping de mots-clés FR/AR → EN pour maximiser les correspondances
    KEYWORD_MAP = {
        # Géopolitique
        "maroc": "morocco", "المغرب": "morocco",
        "gaza": "gaza", "غزة": "gaza", "palestine": "palestine", "فلسطين": "palestine",
        "israel": "israel", "israël": "israel", "إسرائيل": "israel",
        "usa": "us", "états-unis": "us", "etats-unis": "us", "أمريكا": "us",
        "biden": "biden", "trump": "trump",
        "uk": "uk", "britain": "britain", "london": "uk", "bbc": "uk",
        "france": "france", "فرنسا": "france", "macron": "macron",
        "ukraine": "ukraine", "أوكرانيا": "ukraine",
        "russia": "russia", "russie": "russia", "روسيا": "russia",
        "putin": "putin", "poutine": "putin",
        "china": "china", "chine": "china", "chinese": "china", "الصين": "china",
        "xi": "china", "jinping": "china", "beijing": "china",
        "europe": "europe", "eu": "europe", "nato": "nato",
        "iran": "iran", "إيران": "iran", "tehran": "iran",
        "syria": "syria", "syrie": "syria", "سوريا": "syria",
        "lebanon": "lebanon", "liban": "lebanon",
        "india": "india", "inde": "india", "भारत": "india",
        "africa": "africa", "afrique": "africa", "إفريقيا": "africa",
        # Économie / Finance
        "economy": "economy", "économie": "economy", "inflation": "inflation",
        "fed": "fed", "federal": "fed", "reserve": "fed",
        "crypto": "crypto", "bitcoin": "bitcoin", "ethereum": "ethereum",
        "btc": "bitcoin", "eth": "ethereum",
        "stock": "stock", "market": "market", "trade": "trade",
        "tariff": "tariffs", "tarifs": "tariffs", "douane": "tariffs",
        # Sport
        "football": "football", "sport": "sports", "nba": "nba",
        "nhl": "nhl", "world cup": "world cup", "fifa": "fifa",
        # Politique
        "election": "election", "élection": "election",
        "elections": "election", "انتخابات": "election",
        "vote": "election", "president": "president",
        "senate": "senate", "congress": "congress",
        # Thèmes généraux
        "war": "war", "guerre": "war", "حرب": "war",
        "conflict": "war", "military": "military",
        "oil": "oil", "pétrole": "oil", "energy": "energy",
        "nuclear": "nuclear", "nucléaire": "nuclear",
        "ai": "ai", "intelligence": "ai", "artificial": "ai",
        "openai": "openai", "chatgpt": "openai",
        "tech": "technology", "technology": "technology",
        "space": "space", "nasa": "space", "esa": "space",
        "climate": "climate", "climat": "climate",
    }

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._events_cache = None

    def _fetch_all_events(self) -> list[dict]:
        """Charge tous les événements Polymarket actifs (200 max)."""
        if self._events_cache is not None:
            return self._events_cache

        try:
            resp = requests.get(
                self.EVENTS_URL,
                params={"limit": 200, "active": "true", "closed": "false"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            events = resp.json()
            if isinstance(events, list):
                logger.info(f"[Polymarket] {len(events)} événements chargés depuis l'API.")
                self._events_cache = events
                return events
        except Exception as e:
            logger.warning(f"[Polymarket] Impossible de charger les événements : {e}")

        return []

    def _get_markets_for_event(self, event: dict) -> list[dict]:
        """Récupère les marchés d'un événement spécifique."""
        slug = event.get("slug", "")
        ticker = event.get("ticker", "")
        if not slug and not ticker:
            return []

        try:
            params = {"limit": 10, "active": "true", "closed": "false"}
            if slug:
                params["slug"] = slug
            elif ticker:
                params["tag"] = ticker

            resp = requests.get(self.MARKETS_URL, params=params, timeout=self.timeout)
            if resp.ok:
                markets = resp.json()
                if isinstance(markets, list):
                    return markets
        except Exception as e:
            logger.debug(f"[Polymarket] Erreur marchés pour '{slug}': {e}")

        return []

    def _score_event(self, event: dict, search_terms: set[str]) -> float:
        """Score de pertinence d'un événement par rapport à des termes de recherche."""
        title = event.get("title", "").lower()
        if not title:
            return 0.0

        score = 0.0
        for term in search_terms:
            if term in title:
                score += 1.0
            # Bonus pour correspondance exacte de mot
            if f" {term} " in f" {title} ":
                score += 0.5
        return score

    def _best_market_for_event(self, event: dict) -> dict | None:
        """Retourne le meilleur marché (plus gros volume) pour un événement."""
        markets = self._get_markets_for_event(event)
        if not markets:
            # Pas de marchés dédiés, on retourne l'événement lui-même
            return None

        # Trie par volume décroissant
        markets.sort(key=lambda m: float(m.get("volume", 0)), reverse=True)
        return markets[0]

    def _extract_outcome_price(self, market_or_event: dict) -> float:
        """Extrait la probabilité depuis les outcomePrices."""
        prices = market_or_event.get("outcomePrices", "[]")
        if isinstance(prices, str):
            try:
                import ast
                prices = ast.literal_eval(prices)
            except Exception:
                return 0.5
        if isinstance(prices, list) and len(prices) > 0:
            return float(prices[0])
        return 0.5

    def fetch_market_signals(self, keywords: list[str]) -> dict:
        """
        Pour chaque mot-clé (label de topic), trouve le meilleur événement
        Polymarket correspondant via matching local (pas via l'API search).
        """
        signals = {}

        # Charge tous les événements une seule fois
        all_events = self._fetch_all_events()
        if not all_events:
            logger.warning("[Polymarket] Aucun événement disponible.")
            return signals

        # Stop words multilingues à filtrer des labels BERTopic
        STOP_WORDS = {
            'the', 'of', 'to', 'and', 'in', 'is', 'for', 'on', 'with', 'as', 'by',
            'at', 'an', 'be', 'this', 'that', 'from', 'post', 'appeared', 'first',
            'also', 'has', 'have', 'are', 'was', 'were', 'its', 'which', 'his', 'her',
            'they', 'them', 'their', 'but', 'not', 'or', 'so', 'if', 'than', 'because',
            'while', 'where', 'when', 'how', 'what', 'who', 'why', 'about', 'into',
            'over', 'after', 'before', 'between', 'out', 'against', 'during', 'without',
            'under', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'only', 'own',
            'same', 'too', 'very', 'can', 'will', 'just', 'should', 'now', 'said',
            'would', 'could', 'been', 'much', 'many', 'it', 'we', 'you', 'he', 'she',
            'le', 'la', 'les', 'de', 'des', 'un', 'une', 'en', 'que', 'qui', 'pour',
            'dans', 'par', 'sur', 'avec', 'plus', 'ou', 'au', 'aux', 'est', 'sont',
            'cette', 'cet', 'ces', 'son', 'sa', 'ses', 'du', 'dont', 'où', 'quand',
            'comment', 'pourquoi', 'depuis', 'pendant', 'après', 'avant', 'sous',
            'vers', 'selon', 'comme', 'fait', 'pas', 'tout', 'tous', 'être', 'avoir',
            'faire', 'dit', 'mis', 'cet', 'aux', 'says', 'new', 'get', 'one', 'two',
            'في', 'من', 'على', 'الى', 'عن', 'هذا', 'هذه', 'ان', 'او', 'مع', 'كل', 'تم',
            'قد', 'التي', 'الذي', 'بعد', 'بين', 'انه', 'أن', 'إن', 'كان', 'كانت',
            'هو', 'هي', 'هناك', 'كما', 'إلى', 'حول', 'عند', 'لدى', 'عبر', 'نحو', 'منذ',
        }

        for keyword in keywords:
            if not keyword or keyword == "hors-sujet" or keyword.startswith("-1"):
                continue

            # Extraction des termes du label BERTopic
            parts = keyword.replace("_", " ").split()
            raw_terms = parts[1:] if len(parts) > 1 and parts[0].lstrip("-").isdigit() else parts

            if not raw_terms:
                continue

            # Filtre les stop words et les termes trop courts
            meaningful = [t for t in raw_terms if t.lower() not in STOP_WORDS and len(t) >= 2 and not t.isdigit()]

            # Si tout était des stop words, on garde les 2 premiers termes non-numériques
            if not meaningful:
                meaningful = [t for t in raw_terms if len(t) >= 2 and not t.isdigit()][:2]
            if not meaningful:
                continue

            # Traduction FR/AR → EN + construction du set de recherche
            search_terms: set[str] = set()
            for t in meaningful:
                t_lower = t.lower().strip()
                if t_lower in self.KEYWORD_MAP:
                    search_terms.add(self.KEYWORD_MAP[t_lower])
                elif not t_lower.isdigit():
                    search_terms.add(t_lower)

            # Ajoute aussi les termes FR d'origine (au cas où l'event contient du français)
            search_terms.update(t.lower() for t in meaningful if len(t) >= 2 and not t.isdigit())

            if not search_terms:
                continue

            # Scoring de tous les événements
            best_event = None
            best_score = 0.0

            for event in all_events:
                score = self._score_event(event, search_terms)
                if score > best_score:
                    best_score = score
                    best_event = event

            if not best_event or best_score == 0:
                logger.debug(f"[Polymarket] Aucun événement trouvé pour '{keyword}' (termes: {search_terms})")
                continue

            # Récupère le meilleur marché de l'événement
            best_market = self._best_market_for_event(best_event)
            market_data = best_market or best_event

            question = best_event.get("title", market_data.get("question", ""))
            probability = self._extract_outcome_price(market_data)
            volume = float(market_data.get("volume", best_event.get("volume", 0)))

            signals[keyword] = {
                "search_term": ", ".join(sorted(search_terms)),
                "market_question": question,
                "probability": round(probability, 4),
                "probability_pct": f"{probability * 100:.1f}%",
                "volume_usd": volume,
                "url": market_data.get("url", ""),
                "fetched_at": datetime.utcnow().isoformat(),
            }
            logger.info(
                f"[Polymarket] '{keyword}' (score={best_score:.1f}) → "
                f"'{question[:70]}' ({signals[keyword]['probability_pct']})"
            )

        logger.info(f"[Polymarket] {len(signals)} signaux pertinents sur {len(keywords)} topics.")
        return signals

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les colonnes Polymarket au DataFrame.

        Stratégie : mapping article par article via le titre pour trouver
        le meilleur marché Polymarket. Seuls les matchs avec un score >= 2
        sont conservés pour éviter les faux positifs.
        """
        if df.empty:
            return df

        df = df.copy()
        all_events = self._fetch_all_events()

        # Initialise les colonnes
        for col, default in [
            ("polymarket_question", ""), ("polymarket_prob", None),
            ("polymarket_prob_pct", ""), ("polymarket_volume_usd", None),
            ("polymarket_url", ""),
        ]:
            if col not in df.columns:
                df[col] = default

        if not all_events or "titre_clean" not in df.columns:
            return df

        matched = 0
        for idx, row in df.iterrows():
            titre = str(row.get("titre_clean", "")) if pd.notna(row.get("titre_clean")) else ""
            if not titre:
                continue

            # Extraction et filtrage des mots du titre
            title_words = set(w.lower().strip(",.!?:;\"'()[]") for w in titre.split()
                             if len(w) >= 3 and not w.isdigit())
            # Traduction FR/AR → EN
            search_terms = set()
            for w in title_words:
                search_terms.add(self.KEYWORD_MAP.get(w, w))

            if not search_terms:
                continue

            # Scoring de tous les événements
            best_ev = None
            best_score = 0
            for ev in all_events:
                sc = self._score_event(ev, search_terms)
                if sc > best_score:
                    best_score = sc
                    best_ev = ev

            # Seuil minimum de 2.0 pour éviter les faux positifs
            if not best_ev or best_score < 2.0:
                continue

            market = self._best_market_for_event(best_ev)
            md = market or best_ev
            df.at[idx, "polymarket_question"] = best_ev.get("title", md.get("question", ""))
            df.at[idx, "polymarket_prob"] = self._extract_outcome_price(md)
            df.at[idx, "polymarket_prob_pct"] = f"{self._extract_outcome_price(md) * 100:.1f}%"
            df.at[idx, "polymarket_volume_usd"] = float(md.get("volume", best_ev.get("volume", 0)))
            df.at[idx, "polymarket_url"] = md.get("url", "")
            matched += 1

        logger.info(f"[Polymarket] {matched}/{len(df)} articles enrichis avec des signaux marché.")
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

    def __init__(self, gold_root: str = "data/gold", use_minio: bool = False, use_duckdb: bool = True):
        self.gold_root = Path(gold_root)
        self.use_minio = use_minio and MINIO_AVAILABLE
        self.use_duckdb = use_duckdb and DUCKDB_AVAILABLE
        self.bucket_name = "gold"
        self.polymarket = PolymarketEnricher()
        self.duckdb_manager = None

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

        if self.use_duckdb:
            try:
                self.duckdb_manager = DuckDBManager()
                logger.info("[Gold] Connecté au Data Warehouse DuckDB.")
            except Exception as e:
                logger.error(f"[Gold] Erreur connexion DuckDB : {e}")
                self.use_duckdb = False

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

        # 2. Enrichissement Polymarket (matching article par article)
        if enrich_polymarket:
            df = self.polymarket.enrich_dataframe(df)

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
        Génère un résumé JSON des topics (utile pour l'API et le warehouse).

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
        """Sauvegarde la couche Gold en local, sur MinIO et dans le Data Warehouse."""
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

        # Data Warehouse DuckDB
        if self.use_duckdb and self.duckdb_manager is not None:
            try:
                self.duckdb_manager.insert_gold_articles(gold_df)
                self.duckdb_manager.insert_topic_summaries(topic_summaries or [], run_date=date_str)
                self.duckdb_manager.refresh_analytics_tables()
                logger.info("[Gold] Données insérées dans le Data Warehouse.")
            except Exception as e:
                logger.error(f"[Gold] Erreur insertion DuckDB : {e}")

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
