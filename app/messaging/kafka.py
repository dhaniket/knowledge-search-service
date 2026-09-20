import os

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

_producer: AIOKafkaProducer | None = None


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"{name} is not configured")

    return value


def kafka_connection_config() -> dict:

    ca_file = Path(required_env("KAFKA_CA_FILE")).resolve()

    if not ca_file.is_file():
        raise RuntimeError(f"Kafka CA certificate not found: {ca_file}")

    return {
        "bootstrap_servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security_protocol": "SASL_SSL",
        "ssl_context": create_ssl_context(cafile=str(ca_file)),
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": required_env("KAFKA_USERNAME"),
        "sasl_plain_password": required_env("KAFKA_PASSWORD"),
    }


def get_article_event_topic() -> str:
    return os.getenv(
        "KAFKA_ARTICLE_TOPIC",
        "knowledge.article.events.v1",
    )


def get_kafka_publish_timeout() -> float:
    value = float(
        os.getenv(
            "KAFKA_PUBLISH_TIMEOUT_SECONDS",
            "5",
        )
    )

    if value <= 0:
        raise RuntimeError("Kafka publish timeout must be positive")

    return value


async def initialize_kafka() -> None:
    global _producer

    if _producer is not None:
        return

    producer = AIOKafkaProducer(
        **kafka_connection_config(),
        enable_idempotence=True,
    )

    try:
        await producer.start()

    except BaseException:
        await producer.stop()
        raise

    _producer = producer


def get_kafka_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    return _producer


async def close_kafka() -> None:
    global _producer

    producer = _producer
    _producer = None

    if producer is not None:
        await producer.stop()
