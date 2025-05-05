from concurrent import futures
import threading

import grpc
from fastapi import FastAPI, HTTPException
import uvicorn
from grpc_reflection.v1alpha import reflection

from nica_proto import ingest_pb2, ingest_pb2_grpc
from producer import publish_event
from auth_interceptor import JwtInterceptor

# ----------------------------
# FastAPI для HTTP-фасада
# ----------------------------
app = FastAPI()

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/api/v1/events", status_code=202)
def http_ingest(req: dict):
    detector = req.get("detector")
    payload  = req.get("payload")
    if detector is None or payload is None:
        raise HTTPException(status_code=400, detail="detector and payload required")
    try:
        publish_event(detector, payload.encode())
    except Exception as e:
        print(f"[LOCAL DEV] Kafka unavailable, skipping publish: {e}")
    return {"status": "accepted"}

def serve_http():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

# ----------------------------
# gRPC-сервис с JWT-Interceptor
# ----------------------------
class IngestServiceServicer(ingest_pb2_grpc.IngestServiceServicer):
    def IngestEvent(self, request, context):
        publish_event(request.detector, request.payload)
        return ingest_pb2.Ack()

def serve_grpc():
    # JWT-Interceptor: роль "ingest"
    jwt_interceptor = JwtInterceptor(
        issuer="http://localhost:8080/realms/nica-tier1",
        audience="account",
        jwks_url="http://localhost:8080/realms/nica-tier1/protocol/openid-connect/certs",
        required_role="ingest",
    )
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=(jwt_interceptor,),
    )

    ingest_pb2_grpc.add_IngestServiceServicer_to_server(
        IngestServiceServicer(), server
    )

    # Включаем Reflection для grpcurl
    SERVICE_NAMES = (
        "nica_proto.IngestService",
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    server.start()
    print("gRPC server listening on 50051")
    server.wait_for_termination()

# ----------------------------
# Основной запуск
# ----------------------------
if __name__ == "__main__":
    # 1) HTTP сервер на фоне
    threading.Thread(target=serve_http, daemon=True).start()
    # 2) gRPC сервер в основном потоке
    serve_grpc()