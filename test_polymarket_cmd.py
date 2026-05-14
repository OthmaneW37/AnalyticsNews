import sys
from datalake.gold_aggregator import PolymarketEnricher

def main():
    print("=== TESTEUR POLYMARKET EN LIGNE DE COMMANDE ===")
    print("Initialisation de l'enrichisseur PolymarketEnricher...")
    agg = PolymarketEnricher()
    
    # Sujets de test par défaut si aucun argument n'est passé
    test_topics = [
        "0_us_biden_trump",
        "1_gaza_israel_war",
        "2_morocco_sahara_hespress",
        "3_climate_solar_energy",
        "4_ukraine_russia_putin",
        "5_iran_tehran",
    ]
    
    # Si l'utilisateur passe des sujets en arguments de commande
    if len(sys.argv) > 1:
        test_topics = sys.argv[1:]
        
    print(f"\nRecherche de signaux sur les sujets suivants : {test_topics}\n")
    
    signals = agg.fetch_market_signals(test_topics)
    
    print("=== RÉSULTATS DES SIGNAUX POLYMARKET ===")
    if not signals:
        print("Aucun signal trouvé.")
    else:
        for topic, data in signals.items():
            print(f"\nSujet ciblé : {topic}")
            print(f"  ↳ Terme de recherche : '{data['search_term']}'")
            print(f"  ↳ Question du marché : '{data['market_question']}'")
            print(f"  ↳ Probabilité prédite : {data['probability_pct']} ({data['probability']})")
            print(f"  ↳ Volume de trading : {data['volume_usd']} USD")
            if data['url']:
                print(f"  ↳ Lien officiel : https://polymarket.com{data['url']}")

if __name__ == "__main__":
    main()
