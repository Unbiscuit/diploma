from confluent_kafka import Consumer, Producer
import os
import signal
import sys

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
    print("Shutting down gracefully...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def main():
    print("🔧 Filter service starting...")
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
    detector = os.getenv("DETECTOR", "MPD")
    raw_topic = f"{detector}.raw"
    filtered_topic = f"{detector}.filtered"

    consumer.subscribe([raw_topic])
    print(f"Subscribed to {raw_topic}")

    while True:
        print("🕓 Waiting for messages...")
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        print(f"Received: {value}")

        if len(value) > 5:
            print(f"Forwarding message to {filtered_topic}")
            producer.produce(filtered_topic, msg.value())
            producer.flush()
        else:
            print("Filtered out")

if __name__ == "__main__":
    main()