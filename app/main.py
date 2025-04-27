from concurrent import futures
import grpc
from grpc_reflection.v1alpha import reflection
from app.nica_proto import ingest_pb2, ingest_pb2_grpc
from app.producer import publish_event  

class IngestServiceServicer(ingest_pb2_grpc.IngestServiceServicer):
    def IngestEvent(self, request, context):
        publish_event(request.detector, request.payload)
        return ingest_pb2.Ack()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ingest_pb2_grpc.add_IngestServiceServicer_to_server(
        IngestServiceServicer(), server)

    SERVICE_NAMES = (
        'nica_proto.IngestService',
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()