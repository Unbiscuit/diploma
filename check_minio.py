from minio import Minio
from minio.error import S3Error
import os

# Конфиг
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "spd-events")

# Создаем клиента
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

print(f"📡 Connecting to MinIO at {MINIO_ENDPOINT}")

# Проверяем бакет
if not client.bucket_exists(MINIO_BUCKET):
    print(f"❌ Bucket {MINIO_BUCKET} does not exist!")
    exit(1)

print(f"✅ Bucket {MINIO_BUCKET} found.\n📂 Listing objects:")

# Выводим объекты
objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
if not objects:
    print("⚠️ No objects found in the bucket.")
else:
    for obj in objects:
        print(f"📄 Found file: {obj.object_name}")
        response = client.get_object(MINIO_BUCKET, obj.object_name)
        content = response.read().decode("utf-8")
        print(f"📥 Content of {obj.object_name}:\n{content}")
        response.close()
        break  # проверим только первый