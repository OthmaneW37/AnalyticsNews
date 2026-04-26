"""
test_phase1.py
--------------
Script de test rapide pour valider la Phase 1 du pipeline.
Lance un scraping léger (sans fetch_content) pour vérifier que
tous les composants fonctionnent correctement.

Usage :
  python test_phase1.py
  python test_phase1.py --source bbc
  python test_phase1.py --source gdelt --query "Gaza"
"""

import argparse
import json
import sys
import os
from pathlib import Path

# Fix Windows console encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from scrapers.hespress_scraper import HepressScraper
from scrapers.bbc_scraper import BBCScraper
from scrapers.gdelt_client import GDELTClient
from datalake.bronze_writer import BronzeWriter
from datalake.silver_processor import SilverProcessor


def test_scraper(source: str = "bbc", query: str = "Maroc", max_articles: int = 5):
    print(f"\n{'='*60}")
    print(f"  TEST PHASE 1 — Source : {source.upper()}")
    print(f"{'='*60}\n")

    # ------------------------------------------------------------------
    # 1. Scraping (mode rapide : fetch_content=False)
    # ------------------------------------------------------------------
    print("▶ Étape 1 : Scraping...")
    if source == "hespress":
        scraper = HepressScraper(max_per_feed=max_articles, fetch_content=False)
    elif source == "bbc":
        scraper = BBCScraper(max_per_feed=max_articles, fetch_content=False)
    elif source == "gdelt":
        scraper = GDELTClient(query=query, max_records=max_articles, timespan="1d")
    else:
        print(f"❌ Source inconnue : {source}")
        return

    articles = scraper.run()
    print(f"   ✅ {len(articles)} articles récupérés")

    if not articles:
        print("   ⚠️  Aucun article — vérifiez votre connexion internet")
        return

    # Affiche un exemple
    sample = articles[0]
    print(f"\n   Exemple d'article :")
    print(f"   • Titre    : {sample.get('titre', 'N/A')[:80]}")
    print(f"   • Source   : {sample.get('source', 'N/A')}")
    print(f"   • Date     : {sample.get('date_publication', 'N/A')}")
    print(f"   • URL      : {sample.get('url', 'N/A')[:60]}...")

    # ------------------------------------------------------------------
    # 2. Bronze
    # ------------------------------------------------------------------
    print("\n▶ Étape 2 : Écriture Bronze...")
    writer = BronzeWriter(root="data/bronze")
    bronze_path = writer.write(source=source, articles=articles)
    print(f"   ✅ Bronze écrit → {bronze_path}")

    # ------------------------------------------------------------------
    # 3. Silver
    # ------------------------------------------------------------------
    print("\n▶ Étape 3 : Traitement Silver...")
    processor = SilverProcessor(silver_root="data/silver")
    silver_df = processor.process(articles)
    silver_path = processor.save(silver_df, source=source)

    ok = (silver_df["quality_status"] == "OK").sum()
    fail = (silver_df["quality_status"] == "FAIL").sum()
    print(f"   ✅ Silver traité → {silver_path}")
    print(f"   • Qualité OK   : {ok}/{len(silver_df)}")
    print(f"   • Qualité FAIL : {fail}/{len(silver_df)}")

    if fail > 0:
        print("\n   Exemples de FAIL :")
        fails = silver_df[silver_df["quality_status"] == "FAIL"][["titre_clean", "quality_flags"]].head(3)
        for _, row in fails.iterrows():
            print(f"   - {str(row['titre_clean'])[:50]} → {row['quality_flags']}")

    print(f"\n{'='*60}")
    print("  ✅ PHASE 1 OK — Pipeline fonctionnel !")
    print(f"{'='*60}\n")

    print("📁 Structure des données créées :")
    for folder in ["data/bronze", "data/silver"]:
        p = Path(folder)
        if p.exists():
            for f in sorted(p.rglob("*")):
                if f.is_file():
                    size_kb = round(f.stat().st_size / 1024, 1)
                    print(f"   {f}  ({size_kb} KB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Phase 1 du pipeline")
    parser.add_argument("--source", default="bbc", choices=["hespress", "bbc", "gdelt"])
    parser.add_argument("--query", default="Maroc", help="Requête GDELT si source=gdelt")
    parser.add_argument("--max", type=int, default=5, help="Nombre max d'articles")
    args = parser.parse_args()

    test_scraper(source=args.source, query=args.query, max_articles=args.max)
