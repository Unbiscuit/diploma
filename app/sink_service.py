from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from minio import Minio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import time, os, io, uuid, signal, sys

# --- Простенький HTTP-сервер для /healthz ---
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/healthz", "/readyz"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    server = HTTPServer(("", 8080), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

# -------------------------------------------
print("🔧 Sink service starting...")

# Конфиги из env
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio.default.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "spd-events")

# Kafka consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'sink-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}
consumer = Consumer(consumer_conf)

# MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

running = True
def shutdown(sig, frame):
    global running
    print("👋 Shutting down gracefully…")
    running = False
    consumer.close()

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

def main():
    # Запускаем health-поток
    start_health_server()
    print("🚑 Health server started on port 8080 (/healthz /readyz)")

    detector = os.getenv("DETECTOR", "MPD")
    filtered_topic = f"{detector}.filtered"

    consumer.subscribe([filtered_topic])
    print(f"📡 Subscribed to Kafka topic: {filtered_topic}")
    print(f"Connecting to Kafka at    {KAFKA_BOOTSTRAP}")
    print(f"Connecting to MinIO at    {MINIO_ENDPOINT}")

    while running:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            value = msg.value()
            if not value:
                continue

            fname = f"{uuid.uuid4()}.bin"
            stream = io.BytesIO(value)
            print(f"📦 Saving {fname} ({len(value)} bytes)…")

            # На запись в MinIO требуется file-like объект
            minio_client.put_object(
                MINIO_BUCKET,
                fname,
                data=stream,
                length=len(value)
            )
            print(f"✅ Saved: {fname}")

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(2)
            continue

    print("🏁 Sink service stopped.")

if __name__ == "__main__":
    main()