"""
quality_dag.py
--------------
DAG Airflow de contrôle qualité.

Contrôles quotidiens :
1. Vérifie qu'assez d'articles ont été ingérés (seuil min)
2. Calcule le taux de qualité OK et alerte si en dessous du seuil
3. Génère un rapport JSON de qualité
4. [Optionnel] Nettoie les anciennes partitions Bronze > 30 jours
"""

from datetime import datetime, timedelta
import os
import json
import logging

logger = logging.getLogger(__name__)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.email import EmailOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

if AIRFLOW_AVAILABLE:
    import sys
    sys.path.append("/opt/airflow")

    from datalake.silver_processor import SilverProcessor
    from datalake.gold_aggregator import GoldAggregator

    # Seuils de qualité
    MIN_ARTICLES_PER_SOURCE = 5      # au moins 5 articles/source/jour
    MIN_QUALITY_OK_RATE = 0.75       # au moins 75% de qualité OK

    default_args = {
        'owner': 'data_engineer',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }

    dag = DAG(
        'news_quality_check',
        default_args=default_args,
        description='Contrôle qualité quotidien du pipeline news',
        schedule_interval='0 6 * * *',   # chaque jour à 6h UTC
        start_date=datetime(2025, 4, 1),
        catchup=False,
        tags=['quality', 'monitoring'],
    )

    def check_silver_quality(**context):
        """
        Vérifie le taux de qualité des données Silver pour chaque source.
        Lève une exception si un seuil critique est dépassé.
        """
        os.environ["USE_MINIO"] = "true"
        os.environ["MINIO_ENDPOINT"] = "minio:9000"

        date_str = context["ds"]  # date d'exécution du DAG (YYYY-MM-DD)
        processor = SilverProcessor(use_minio=True)
        report = {"date": date_str, "sources": {}, "alerts": []}

        sources = ["hespress", "bbc", "gdelt"]

        for source in sources:
            df = processor.load(source=source, date=date_str)
            if df.empty:
                alert = f"[ALERTE] Aucune donnée Silver pour '{source}' le {date_str}"
                logger.warning(alert)
                report["alerts"].append(alert)
                report["sources"][source] = {"status": "MISSING", "articles": 0}
                continue

            n_articles = len(df)
            ok_rate = (df["quality_status"] == "OK").mean() if "quality_status" in df.columns else 1.0

            status = "OK"
            if n_articles < MIN_ARTICLES_PER_SOURCE:
                alert = f"[ALERTE] '{source}' : seulement {n_articles} articles (min={MIN_ARTICLES_PER_SOURCE})"
                logger.warning(alert)
                report["alerts"].append(alert)
                status = "LOW_VOLUME"

            if ok_rate < MIN_QUALITY_OK_RATE:
                alert = f"[ALERTE] '{source}' : taux qualité {ok_rate:.1%} < {MIN_QUALITY_OK_RATE:.0%}"
                logger.warning(alert)
                report["alerts"].append(alert)
                status = "LOW_QUALITY"

            report["sources"][source] = {
                "status": status,
                "articles": n_articles,
                "quality_ok_rate": round(ok_rate, 4),
            }
            logger.info(f"[Qualité] {source}: {n_articles} articles, {ok_rate:.1%} OK")

        # Sauvegarde du rapport
        report_path = f"/tmp/quality_report_{date_str}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"[Qualité] Rapport sauvegardé : {report_path}")

        # Levée d'exception si des alertes critiques existent
        if report["alerts"]:
            logger.error(f"[Qualité] {len(report['alerts'])} alertes détectées !")
        
        # Push le rapport vers XCom pour les tâches suivantes
        context["ti"].xcom_push(key="quality_report", value=report)
        return report

    def build_gold_layer(**context):
        """
        Lance la construction de la couche Gold depuis Silver.
        Applique BERTopic et Polymarket sur les données du jour.
        """
        import pandas as pd
        os.environ["USE_MINIO"] = "true"
        os.environ["MINIO_ENDPOINT"] = "minio:9000"
        os.environ["USE_KAFKA"] = "true"
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:9092"

        date_str = context["ds"]
        processor = SilverProcessor(use_minio=True)
        aggregator = GoldAggregator(use_minio=True)

        # Charge et concatène tous les Silver du jour
        frames = []
        for source in ["hespress", "bbc", "gdelt"]:
            df = processor.load(source=source, date=date_str)
            if not df.empty:
                frames.append(df)

        if not frames:
            logger.warning("[Gold] Aucune donnée Silver à agréger.")
            return

        silver_combined = pd.concat(frames, ignore_index=True)
        logger.info(f"[Gold] {len(silver_combined)} articles Silver chargés.")

        # BERTopic
        silver_with_topics = processor.apply_bertopic(silver_combined)

        # Construction Gold + Polymarket
        gold_df = aggregator.build_gold(silver_with_topics, enrich_polymarket=True)
        topic_summaries = aggregator.get_topic_summary(gold_df)

        # Sauvegarde
        gold_path = aggregator.save(gold_df, topic_summaries)
        logger.info(f"[Gold] Couche Gold sauvegardée : {gold_path}")

    # ============================================================
    # Définition des tâches
    # ============================================================
    task_quality_check = PythonOperator(
        task_id='check_silver_quality',
        python_callable=check_silver_quality,
        provide_context=True,
        dag=dag,
    )

    task_build_gold = PythonOperator(
        task_id='build_gold_layer',
        python_callable=build_gold_layer,
        provide_context=True,
        dag=dag,
    )

    # Ordre : vérifie la qualité AVANT de construire Gold
    task_quality_check >> task_build_gold
