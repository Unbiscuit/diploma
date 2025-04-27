from confluent_kafka import Consumer
from minio import Minio
import os
import uuid

# Конфиги
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "spd-events")

# Kafka Consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'ingest-2-minio-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# Minio Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # без https внутри кластера
)

def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists")

def main():
    detector = os.getenv("DETECTOR", "MPD")
    filtered_topic = f"{detector}.filtered"

    consumer.subscribe([filtered_topic])
    print(f"Subscribed to {filtered_topic}")
    ensure_bucket(MINIO_BUCKET)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        payload = msg.value()
        file_name = f"{uuid.uuid4()}.bin"

        # Сохраняем в файл
        with open(file_name, "wb") as f:
            f.write(payload)

        # Заливаем в MinIO
        minio_client.fput_object(
            MINIO_BUCKET,
            file_name,
            file_name
        )
        print(f"Uploaded {file_name} to bucket {MINIO_BUCKET}")

        # Удаляем локальный файл
        os.remove(file_name)

if __name__ == "__main__":
    main()

