from confluent_kafka import Producer
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
producer = Producer(conf)

def publish_event(detector: str, data: bytes):
    try:
        topic = f"{detector}.raw"
        # отправляем — но не ждём бесконечно
        producer.produce(topic, value=data)
        # обходим блокировку, даём 0.1сек на отправку
        producer.flush(timeout=0.1)
    except Exception as e:
        print(f"[LOCAL DEV] Kafka unavailable (or timeout), skipping publish: {e}")