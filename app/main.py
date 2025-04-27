from concurrent import futures
import grpc
from fastapi import FastAPI
import threading
import uvicorn
from grpc_reflection.v1alpha import reflection
from app.nica_proto import ingest_pb2, ingest_pb2_grpc
from app.producer import publish_event

# Создаём FastAPI приложение для health-чека
health_app = FastAPI()

@health_app.get("/healthz")
def healthz():
    return {"status": "ok"}

class IngestServiceServicer(ingest_pb2_grpc.IngestServiceServicer):
    def IngestEvent(self, request, context):
        publish_event(request.detector, request.payload)
        return ingest_pb2.Ack()

def start_http_server():
    uvicorn.run(health_app, host="0.0.0.0", port=8080, log_level="warning")

def serve():
    # Параллельно запускаем HTTP сервер для пробы
    threading.Thread(target=start_http_server, daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ingest_pb2_grpc.add_IngestServiceServicer_to_server(IngestServiceServicer(), server)

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