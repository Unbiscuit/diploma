#!/usr/bin/env python3
import os
import time
import uuid
import signal
import sys
import threading
import io
from confluent_kafka import Consumer
from minio import Minio
import h5py
from http.server import HTTPServer, BaseHTTPRequestHandler

# —————————————————————————————————————————————————————————————————————————
# 1. Конфиги через ENV (ставь себе при тесте SLICE_SEC=10 SLICE_COUNT=100)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.default.svc.cluster.local:9092")
DETECTOR       = os.getenv("DETECTOR",       "MPD")
SLICE_SEC      = int(os.getenv("SLICE_SEC",   "10"))
SLICE_COUNT    = int(os.getenv("SLICE_COUNT", "100"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",    "spd-events")

RAW_TOPIC      = f"{DETECTOR}.filtered"
OUT_PATH       = "/data/slices"  # локальный каталог для срезов

# —————————————————————————————————————————————————————————————————————————
# 2. Health HTTP-сервер
class HealthCheck(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/healthz", "/readyz"):
            self.send_response(200)
            self.end_headers(); self.wfile.write(b"OK")
        else:
            self.send_response(404); self.end_headers()

def start_health():
    srv = HTTPServer(("0.0.0.0", 8080), HealthCheck)
    print("🩺 Health on 0.0.0.0:8080")
    srv.serve_forever()

# —————————————————————————————————————————————————————————————————————————
# 3. Graceful shutdown
running = True
def shutdown(sig, frame):
    global running
    print("👋 Shutdown")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# —————————————————————————————————————————————————————————————————————————
# 4. Init Kafka consumer + MinIO client
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': f"builder-{DETECTOR}",
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([RAW_TOPIC])
print(f"🔧 Subscribed to {RAW_TOPIC} @ {KAFKA_BOOTSTRAP}")

minio = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False
)
print(f"📦 MinIO → {MINIO_ENDPOINT}/{MINIO_BUCKET}")

# —————————————————————————————————————————————————————————————————————————
def make_h5(filename, records):
    """Записываем список кортежей (uuid, ts, payload) в HDF5."""
    path = os.path.join(OUT_PATH, filename)
    with h5py.File(path, "w") as f:
        grp = f.create_group("events")
        # строки сохраняем в special dtype:
        dt_str = h5py.string_dtype(encoding='utf-8')
        grp.create_dataset("id",      data=[r[0] for r in records], dtype=dt_str)
        grp.create_dataset("ts",      data=[r[1] for r in records], dtype=dt_str)
        grp.create_dataset("payload", data=[r[2] for r in records], dtype=dt_str)
    print(f"✅ HDF5 written: {path}")
    return path

# —————————————————————————————————————————————————————————————————————————
def main_loop():
    seq = 0
    buffer = []
    ts0 = time.time()

    while running:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        raw = msg.value().decode("utf-8")
        
        if "|" not in raw:
            print(f"⚠️ Builder: malformed message (no delimiter), skipping: «{raw}»")
            continue
        # разобьём по первой │
        uid = str(uuid.uuid4())
        ts, payload = raw.split("|", 1)
        buffer.append((uid, ts, payload))

        # условие среза
        if (time.time() - ts0 >= SLICE_SEC) or (len(buffer) >= SLICE_COUNT):
            # имя: spd-MPD-YYYYMMDD-HHMMSS-<seq>.h5
            tstamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(ts0))
            fname  = f"spd-{DETECTOR}-{tstamp}-{seq}.h5"
            seq += 1

            # 1) локальный HDF5
            local = make_h5(fname, buffer)

            # 2) пушим в MinIO
            size = os.path.getsize(local)
            with open(local, "rb") as fd:
                minio.put_object(MINIO_BUCKET, fname, data=fd, length=size)
            print(f"🚀 Uploaded to MinIO: {fname} ({size} B)")

            # 3) очистка
            buffer.clear()
            ts0 = time.time()

    consumer.close()
    print("🏁 Builder stopped")

if __name__ == "__main__":
    # убедимся, что папка есть
    os.makedirs(OUT_PATH, exist_ok=True)

    # стартуем health
    threading.Thread(target=start_health, daemon=True).start()
    # запускаем main
    main_loop()
