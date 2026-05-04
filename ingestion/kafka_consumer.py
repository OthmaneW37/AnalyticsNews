"""
ingestion/kafka_consumer.py
---------------------------
Consumer Kafka pour le streaming ingestion.

Pour chaque événement 'articles_ingested' reçu :
1. Lit le fichier Bronze correspondant
2. Applique le traitement Silver
3. Sauvegarde en local / MinIO
4. Met à jour le Data Warehouse DuckDB (optionnel)

Usage :
    python -m ingestion.kafka_consumer
    python -m ingestion.kafka_consumer --topic news_events --group news-consumer
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_consumer")

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("kafka-python non installé. Consumer indisponible.")

from datalake.bronze_writer import BronzeWriter
from datalake.silver_processor import SilverProcessor
from warehouse.duckdb_manager import DuckDBManager

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news_events")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "news_pipeline_consumer")


class NewsKafkaConsumer:
    """
    Consumer Kafka qui traite les articles en temps réel.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_GROUP,
        auto_offset_reset: str = "earliest",
    ):
        self.topic = topic
        self.consumer = None
        self.silver_processor = SilverProcessor()
        self.duckdb = DuckDBManager()

        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python est requis. Installez-le avec : pip install kafka-python")

        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=True,
            )
            logger.info(f"[Consumer] Connecté à Kafka {bootstrap_servers} — topic '{topic}' — group '{group_id}'")
        except Exception as e:
            logger.error(f"[Consumer] Erreur connexion Kafka : {e}")
            raise

    def run(self):
        """Boucle de consommation."""
        logger.info("[Consumer] En attente de messages... (Ctrl+C pour arrêter)")
        try:
            for message in self.consumer:
                self._process_message(message.value)
        except KeyboardInterrupt:
            logger.info("[Consumer] Arrêt demandé par l'utilisateur.")
        finally:
            self.close()

    def _process_message(self, event: dict):
        """Traite un événement d'ingestion."""
        event_type = event.get("event")
        if event_type != "articles_ingested":
            logger.debug(f"[Consumer] Événement ignoré : {event_type}")
            return

        source = event.get("source", "unknown")
        count = event.get("count", 0)
        bronze_path = event.get("bronze_path", "")
        timestamp = event.get("timestamp", "")

        logger.info(f"[Consumer] Événement reçu : {source} ({count} articles) — {bronze_path}")

        # 1. Lecture Bronze
        articles = self._read_bronze(bronze_path)
        if not articles:
            logger.warning(f"[Consumer] Aucun article lu depuis {bronze_path}")
            return

        # 2. Traitement Silver
        silver_df = self.silver_processor.process(articles)
        if silver_df.empty:
            logger.warning("[Consumer] DataFrame Silver vide après traitement.")
            return

        # 3. Sauvegarde Silver
        silver_path = self.silver_processor.save(silver_df, source=source)
        logger.info(f"[Consumer] Silver sauvegardé → {silver_path}")

        # 4. Insertion Data Warehouse
        try:
            self.duckdb.insert_gold_articles(silver_df)
            self.duckdb.refresh_analytics_tables()
            logger.info("[Consumer] Data Warehouse mis à jour.")
        except Exception as e:
            logger.error(f"[Consumer] Erreur DuckDB : {e}")

    def _read_bronze(self, bronze_path: str) -> list[dict]:
        """Lit un fichier Bronze local ou MinIO."""
        if bronze_path.startswith("s3://"):
            # MinIO : on utilise BronzeWriter pour lire
            writer = BronzeWriter(use_minio=True)
            parts = bronze_path.replace("s3://bronze/", "").split("/")
            if len(parts) >= 2:
                source = parts[0]
                date = parts[1]
                return writer.read_all(source=source, date=date)
            return []
        else:
            # Local
            path = Path(bronze_path)
            if not path.exists():
                return []
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload.get("articles", [])

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("[Consumer] Connexion Kafka fermée.")
        self.duckdb.close()


def main():
    parser = argparse.ArgumentParser(description="Consumer Kafka — Streaming Ingestion")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--group", default=KAFKA_GROUP)
    parser.add_argument("--offset-reset", default="earliest", choices=["earliest", "latest"])
    args = parser.parse_args()

    consumer = NewsKafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group,
        auto_offset_reset=args.offset_reset,
    )
    consumer.run()


if __name__ == "__main__":
    main()
