import grpc
import jwt
from jwt import PyJWKClient
from grpc import StatusCode

class JwtInterceptor(grpc.ServerInterceptor):
    def __init__(self, issuer: str, audience: str, jwks_url: str, required_role: str):
        self.issuer = issuer
        self.audience = audience
        self.required_role = required_role
        self.jwks_client = PyJWKClient(jwks_url)

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method or ""
        if method.startswith("/grpc.reflection.v1alpha.ServerReflection/"):
            return continuation(handler_call_details)

        md = dict(handler_call_details.invocation_metadata or ())
        auth = md.get("authorization", "")
        if not auth.startswith("Bearer "):
            def unauth(request, context):
                context.abort(StatusCode.UNAUTHENTICATED, "Missing Bearer token")
            return grpc.unary_unary_rpc_method_handler(unauth)

        token = auth.split(" ", 1)[1]
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token).key
            payload = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
            )
        except Exception as err:
            # сохраняем сообщение об ошибке в замыкании
            error_message = f"Invalid token: {err}"
            def unauth(request, context):
                context.abort(StatusCode.UNAUTHENTICATED, error_message)
            return grpc.unary_unary_rpc_method_handler(unauth)

        roles = payload.get("realm_access", {}).get("roles", [])
        if self.required_role not in roles:
            def forbidden(request, context):
                context.abort(StatusCode.PERMISSION_DENIED, "Permission denied")
            return grpc.unary_unary_rpc_method_handler(forbidden)

        # Всё ок — пропускаем вызов дальше
        return continuation(handler_call_details)