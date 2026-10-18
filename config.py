"""Local Kafka settings. No credentials are needed for this teaching broker."""
import json
import os
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
SUFFIX = os.getenv("KAFKA_TOPIC_SUFFIX", "aurika_hryhorova_hw05")
SENSORS = f"building_sensors_{SUFFIX}"
TEMPERATURE = f"temperature_alerts_{SUFFIX}"
HUMIDITY = f"humidity_alerts_{SUFFIX}"


def encode(value):
    return json.dumps(value).encode("utf-8")


def decode(value):
    return json.loads(value.decode("utf-8"))


def make_producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         value_serializer=encode, acks="all", retries=3)
