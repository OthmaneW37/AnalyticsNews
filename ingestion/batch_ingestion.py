import argparse
import logging
import sys
import time
import os
from pathlib import Path

# Ajoute la racine du projet au path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.hespress_scraper import HepressScraper
from scrapers.bbc_scraper import BBCScraper
from scrapers.gdelt_client import GDELTClient
from datalake.bronze_writer import BronzeWriter
from datalake.silver_processor import SilverProcessor
from ingestion.kafka_producer import NewsKafkaProducer  # noqa: E402 — ajouté après sys.path.insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("batch_ingestion")


def run_pipeline(
    sources: list[str] = None,
    gdelt_query: str = "Maroc",
    gdelt_timespan: str = "6h",
    gdelt_lang: str = "french",
    bronze_root: str = "data/bronze",
    silver_root: str = "data/silver",
    max_per_feed: int = 20,
    fetch_content: bool = True,
    use_minio: bool = False,
    use_kafka: bool = False,
) -> dict:
    """
    Exécute le pipeline d'ingestion batch complet.
    """
    sources = sources or ["hespress", "bbc", "gdelt"]
    bronze_writer = BronzeWriter(root=bronze_root, use_minio=use_minio)
    silver_processor = SilverProcessor(silver_root=silver_root, use_minio=use_minio)
    kafka_producer = NewsKafkaProducer(use_kafka=use_kafka)

    stats = {}
    start_total = time.time()

    for source in sources:
        logger.info(f"\n{'='*60}")
        logger.info(f"DÉMARRAGE SOURCE : {source.upper()}")
        logger.info(f"{'='*60}")
        start = time.time()

        # 1. SCRAPING
        scraper = _get_scraper(
            source=source,
            gdelt_query=gdelt_query,
            gdelt_timespan=gdelt_timespan,
            gdelt_lang=gdelt_lang,
            max_per_feed=max_per_feed,
            fetch_content=fetch_content,
        )
        if scraper is None:
            continue

        articles = scraper.run()

        # 2. ÉCRITURE BRONZE
        bronze_path = bronze_writer.write(source=source, articles=articles)

        # 3. KAFKA EVENT
        if articles and bronze_path:
            kafka_producer.send_ingestion_event(
                source=source, 
                count=len(articles), 
                bronze_path=str(bronze_path)
            )

        # 4. TRAITEMENT SILVER
        silver_df = silver_processor.process(articles)
        silver_path = silver_processor.save(df=silver_df, source=source)

        elapsed = round(time.time() - start, 2)
        ok_count = int((silver_df["quality_status"] == "OK").sum()) if not silver_df.empty else 0
        fail_count = int((silver_df["quality_status"] == "FAIL").sum()) if not silver_df.empty else 0

        stats[source] = {
            "articles_scraped": len(articles),
            "quality_ok": ok_count,
            "quality_fail": fail_count,
            "bronze_path": str(bronze_path),
            "silver_path": str(silver_path),
            "elapsed_seconds": elapsed,
        }

        logger.info(f"[{source}] ✅ Terminé en {elapsed}s — {ok_count} OK / {fail_count} FAIL")

    total_elapsed = round(time.time() - start_total, 2)
    logger.info(f"\n{'='*60}")
    logger.info(f"PIPELINE COMPLET en {total_elapsed}s")
    _print_summary(stats)
    
    kafka_producer.close()

    return stats


# ------------------------------------------------------------------
# Factory de scrapers
# ------------------------------------------------------------------

def _get_scraper(
    source: str,
    gdelt_query: str,
    gdelt_timespan: str,
    gdelt_lang: str,
    max_per_feed: int,
    fetch_content: bool,
):
    if source == "hespress":
        return HepressScraper(
            max_per_feed=max_per_feed,
            fetch_content=fetch_content,
        )
    elif source == "bbc":
        return BBCScraper(
            max_per_feed=max_per_feed,
            fetch_content=fetch_content,
        )
    elif source == "gdelt":
        return GDELTClient(
            query=gdelt_query,
            max_records=250,
            timespan=gdelt_timespan,
            sourcelang=gdelt_lang,
        )
    return None


# ------------------------------------------------------------------
# Affichage du résumé
# ------------------------------------------------------------------

def _print_summary(stats: dict):
    logger.info("\n📊 RÉSUMÉ DU PIPELINE :")
    logger.info(f"{'Source':<12} {'Articles':>10} {'OK':>8} {'FAIL':>8} {'Temps':>10}")
    logger.info("-" * 52)
    for source, s in stats.items():
        logger.info(
            f"{source:<12} {s['articles_scraped']:>10} "
            f"{s['quality_ok']:>8} {s['quality_fail']:>8} "
            f"{s['elapsed_seconds']:>9}s"
        )


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline d'ingestion batch — News Pipeline Phase 2"
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["hespress", "bbc", "gdelt"],
        choices=["hespress", "bbc", "gdelt"],
        help="Sources à scraper (défaut : toutes)",
    )
    parser.add_argument(
        "--gdelt-query",
        default="Maroc",
        help="Requête GDELT (ex: 'Maroc économie')",
    )
    parser.add_argument(
        "--gdelt-timespan",
        default="6h",
        choices=["1h", "6h", "1d", "1w"],
        help="Fenêtre temporelle GDELT",
    )
    parser.add_argument(
        "--gdelt-lang",
        default="french",
        choices=["french", "english", "arabic"],
        help="Langue des sources GDELT",
    )
    parser.add_argument(
        "--max-per-feed",
        type=int,
        default=20,
        help="Nombre max d'articles par flux RSS",
    )
    parser.add_argument(
        "--no-content",
        action="store_true",
        help="Désactive la récupération du contenu complet (plus rapide)",
    )
    parser.add_argument(
        "--bronze-root",
        default="data/bronze",
        help="Chemin racine Bronze (local)",
    )
    parser.add_argument(
        "--silver-root",
        default="data/silver",
        help="Chemin racine Silver (local)",
    )
    parser.add_argument(
        "--use-minio",
        action="store_true",
        help="Utilise MinIO pour le stockage (Phase 2)",
    )
    parser.add_argument(
        "--use-kafka",
        action="store_true",
        help="Utilise Kafka pour les événements (Phase 2)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    # Override par variables d'environnement si présentes (pour Airflow/Docker)
    use_minio = args.use_minio or os.getenv("USE_MINIO", "false").lower() == "true"
    use_kafka = args.use_kafka or os.getenv("USE_KAFKA", "false").lower() == "true"

    run_pipeline(
        sources=args.sources,
        gdelt_query=args.gdelt_query,
        gdelt_timespan=args.gdelt_timespan,
        gdelt_lang=args.gdelt_lang,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        max_per_feed=args.max_per_feed,
        fetch_content=not args.no_content,
        use_minio=use_minio,
        use_kafka=use_kafka,
    )
