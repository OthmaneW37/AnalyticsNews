"""
run_full_pipeline.py
---------------------
Script tout-en-un pour exécuter les 4 phases du pipeline
sans avoir besoin de Docker (mode local).

Usage :
  python -X utf8 run_full_pipeline.py
  python -X utf8 run_full_pipeline.py --source bbc --no-bertopic
  python -X utf8 run_full_pipeline.py --source bbc hespress --no-polymarket
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("full_pipeline")

from ingestion.batch_ingestion import run_pipeline
from datalake.silver_processor import SilverProcessor
from datalake.gold_aggregator import GoldAggregator
import pandas as pd


def main(
    sources: list[str],
    gdelt_query: str,
    max_per_feed: int,
    apply_bertopic: bool,
    apply_polymarket: bool,
):
    print("\n" + "=" * 65)
    print("  NEWS PIPELINE — EXÉCUTION COMPLÈTE (LOCAL)")
    print("  Phases 1 → 4 sans Docker")
    print("=" * 65 + "\n")

    # --------------------------------------------------------
    # PHASE 1 & 2 (local) : Scraping → Bronze → Silver
    # --------------------------------------------------------
    print("▶ PHASE 1+2 : Scraping + Bronze + Silver...")
    stats = run_pipeline(
        sources=sources,
        gdelt_query=gdelt_query,
        max_per_feed=max_per_feed,
        fetch_content=False,
        use_minio=False,
        use_kafka=False,
    )
    print(f"   ✅ {sum(s['articles_scraped'] for s in stats.values())} articles scrapés\n")

    # --------------------------------------------------------
    # PHASE 3 : BERTopic + Polymarket → Gold
    # --------------------------------------------------------
    print("▶ PHASE 3 : BERTopic + Polymarket → Gold...")
    processor = SilverProcessor(silver_root="data/silver")
    aggregator = GoldAggregator(gold_root="data/gold")

    # Charge tous les Silver du jour
    frames = []
    for source in sources:
        df = processor.load(source=source)
        if not df.empty:
            frames.append(df)
            print(f"   Silver chargé pour {source} : {len(df)} articles")

    if not frames:
        print("   ⚠️  Aucune donnée Silver — pipeline arrêté.")
        return

    silver_df = pd.concat(frames, ignore_index=True)
    print(f"   Total Silver : {len(silver_df)} articles")

    # BERTopic
    if apply_bertopic:
        print("   Lancement BERTopic (peut prendre 1-2 min selon le CPU)...")
        silver_df = processor.apply_bertopic(silver_df)
        n_topics = silver_df["topic_id"].nunique() if "topic_id" in silver_df.columns else 0
        print(f"   ✅ BERTopic : {n_topics} topics détectés")
    else:
        silver_df["topic_id"] = 0
        silver_df["topic_label"] = "non-modélisé"
        silver_df["topic_prob"] = 1.0
        print("   ⏭  BERTopic ignoré (--no-bertopic)")

    # Gold + Polymarket
    print("   Construction Gold + Polymarket...")
    gold_df = aggregator.build_gold(silver_df, enrich_polymarket=apply_polymarket)
    topic_summaries = aggregator.get_topic_summary(gold_df)
    gold_path = aggregator.save(gold_df, topic_summaries)
    enriched = gold_df["polymarket_prob"].notna().sum() if "polymarket_prob" in gold_df.columns else 0
    print(f"   ✅ Gold sauvegardé → {gold_path}")
    print(f"   ✅ {len(topic_summaries)} topics résumés | {enriched} signaux Polymarket\n")

    # --------------------------------------------------------
    # PHASE 4 : Instructions Dashboard
    # --------------------------------------------------------
    print("▶ PHASE 4 : Dashboard Streamlit")
    print("   Pour lancer le dashboard, exécutez :")
    print("   ┌─────────────────────────────────────────────┐")
    print("   │  streamlit run dashboard/app.py             │")
    print("   └─────────────────────────────────────────────┘")
    print("   (Installez d'abord : pip install streamlit plotly)\n")

    # --------------------------------------------------------
    # RÉSUMÉ FINAL
    # --------------------------------------------------------
    print("=" * 65)
    print("  ✅ PIPELINE COMPLET TERMINÉ")
    print(f"     Articles scrapés  : {sum(s['articles_scraped'] for s in stats.values())}")
    print(f"     Articles Gold OK  : {(gold_df['quality_status'] == 'OK').sum() if 'quality_status' in gold_df.columns else len(gold_df)}")
    print(f"     Topics détectés   : {len(topic_summaries)}")
    print(f"     Signaux Polymarket: {enriched}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline complet local — Phases 1 à 4")
    parser.add_argument("--source", nargs="+", default=["bbc"],
                        choices=["hespress", "bbc", "gdelt"],
                        help="Sources à scraper")
    parser.add_argument("--gdelt-query", default="Maroc",
                        help="Requête GDELT")
    parser.add_argument("--max-per-feed", type=int, default=15,
                        help="Articles max par flux")
    parser.add_argument("--no-bertopic", action="store_true",
                        help="Désactive BERTopic (plus rapide)")
    parser.add_argument("--no-polymarket", action="store_true",
                        help="Désactive Polymarket (mode offline)")
    args = parser.parse_args()

    main(
        sources=args.source,
        gdelt_query=args.gdelt_query,
        max_per_feed=args.max_per_feed,
        apply_bertopic=not args.no_bertopic,
        apply_polymarket=not args.no_polymarket,
    )
