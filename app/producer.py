from confluent_kafka import Producer
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
producer = Producer(conf)

def publish_event(detector: str, data: bytes):
    topic = f"{detector}.raw"
    producer.produce(topic, value=data)
    producer.flush()