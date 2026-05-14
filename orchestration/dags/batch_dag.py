"""
batch_dag.py
------------
DAG Airflow pour orchestrer le pipeline d'ingestion.
Il utilise `batch_ingestion.py` et active MinIO + Kafka.
"""

from datetime import datetime, timedelta
import os

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

if AIRFLOW_AVAILABLE:
    # On ajoute le répertoire racine au PYTHONPATH pour trouver `ingestion`
    import sys
    sys.path.append("/opt/airflow")

    from ingestion.batch_ingestion import run_pipeline

    default_args = {
        'owner': 'data_engineer',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        'news_batch_ingestion',
        default_args=default_args,
        description='Scrape les articles, écrit dans MinIO (Bronze/Silver) et envoie à Kafka',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2025, 4, 1),
        catchup=False,
        tags=['news', 'ingestion', 'bronze', 'silver'],
    )

    def scrape_source(source: str):
        # On force les variables d'environnement pour MinIO et Kafka (configurées dans Docker)
        os.environ["USE_MINIO"] = "true"
        os.environ["USE_KAFKA"] = "true"
        os.environ["MINIO_ENDPOINT"] = "minio:9000"
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:9092"
        
        run_pipeline(
            sources=[source],
            use_minio=True,
            use_kafka=True,
            fetch_content=False  # Rapide pour la démo
        )

    # Tâches
    task_hespress = PythonOperator(
        task_id='scrape_hespress',
        python_callable=scrape_source,
        op_kwargs={'source': 'hespress'},
        dag=dag,
    )

    task_bbc = PythonOperator(
        task_id='scrape_bbc',
        python_callable=scrape_source,
        op_kwargs={'source': 'bbc'},
        dag=dag,
    )

    task_gdelt = PythonOperator(
        task_id='scrape_gdelt',
        python_callable=scrape_source,
        op_kwargs={'source': 'gdelt'},
        dag=dag,
    )

    # Les tâches sont indépendantes → s'exécutent en parallèle dans Airflow (pas de >> nécessaire)
    # Pour les chaîner séquentiellement, utilisez : task_hespress >> task_bbc >> task_gdelt
