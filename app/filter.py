from confluent_kafka import Consumer, Producer
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'filter-group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

def shutdown(sig, frame):
    print("🔴 Shutting down gracefully...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

def start_http_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    print("🩺 Health server running on port 8080")
    # Запускаем serve_forever в фоне
    threading.Thread(target=server.serve_forever, daemon=True).start()

def main():
    detector     = os.getenv("DETECTOR", "MPD")
    raw_topic    = f"{detector}.raw"
    filt_topic   = f"{detector}.filtered"

    # 1) старт HTTP в фоне
    start_http_server()

    # 2) подписка на Kafka
    consumer.subscribe([raw_topic])
    print(f"✅ Subscribed to topic {raw_topic}")
    print("👀 Polling messages...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        val = msg.value().decode('utf-8')
        print(f"▶️ Received: {val}")

        _, payload = val.split("|", 1) if "|" in val else ("", "")
        if len(payload) > 5:
            producer.produce(filt_topic, msg.value())
            producer.flush()
            print(f"✅ Forwarded to {filt_topic}")
        else:
            print("🚫 Filtered out")

if __name__ == "__main__":
    print("🔧 Filter service starting...")
    main()
