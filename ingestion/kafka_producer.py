"""
kafka_producer.py
-----------------
Envoie des événements à Kafka lors de l'ingestion.

Exemple de message :
{
    "event": "articles_ingested",
    "source": "bbc",
    "count": 24,
    "bronze_path": "s3://bronze/bbc/2026-04-26/...",
    "timestamp": "2026-04-26T12:00:00"
}
"""

import json
import logging
import os
from datetime import datetime

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news_events")


class NewsKafkaProducer:
    """
    Producteur Kafka pour les événements d'ingestion.
    """

    def __init__(self, use_kafka: bool = False):
        self.use_kafka = use_kafka and KAFKA_AVAILABLE
        self.producer = None

        if self.use_kafka:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                logger.info(f"[Kafka] Connecté à {KAFKA_BOOTSTRAP_SERVERS}")
            except Exception as e:
                logger.error(f"[Kafka] Erreur connexion : {e}")
                self.use_kafka = False

    def send_ingestion_event(self, source: str, count: int, bronze_path: str):
        """Envoie un événement d'ingestion à Kafka."""
        if not self.use_kafka or self.producer is None:
            return

        event = {
            "event": "articles_ingested",
            "source": source,
            "count": count,
            "bronze_path": str(bronze_path),
            "timestamp": datetime.utcnow().isoformat()
        }

        try:
            self.producer.send(KAFKA_TOPIC, event)
            self.producer.flush()
            logger.info(f"[Kafka] Événement envoyé pour {source} ({count} articles)")
        except Exception as e:
            logger.error(f"[Kafka] Erreur d'envoi : {e}")

    def close(self):
        if self.producer:
            self.producer.close()
