import grpc
from app.nica_proto import ingest_pb2
from app.nica_proto import ingest_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = ingest_pb2_grpc.IngestServiceStub(channel)

    event = ingest_pb2.IngestRequest(
    detector="MPD",
    payload=b"2025-04-27T21:30:00|this is a very long payload that is BIG",
    )
    response = stub.IngestEvent(event)
    print("Ingest response received")

if __name__ == "__main__":
    run()