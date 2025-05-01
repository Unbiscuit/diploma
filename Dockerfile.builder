FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем только нужные модули
COPY app/nica_proto ./app/nica_proto
COPY app/producer.py app/filter.py app/ingest_2_minio.py app/sink_service.py app/main.py app/builder.py ./app/

CMD ["python", "-u", "-m", "app.builder"]