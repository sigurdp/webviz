import logging

from fastapi import APIRouter, HTTPException, Request, Response
from cryptography.fernet import InvalidToken
from pydantic_core import ValidationError
from webviz_server_schemas.internal.acquire_access_token import AccessTokenRequest, AccessTokenResponse
from webviz_services.utils.dispatch_token_store import DispatchTokenStoreSingleton, DispatchTokenStore
from webviz_services.utils.payload_encryption import PayloadEncryption

from primary import config
from primary.auth.auth_helper import get_sumo_access_token_for_session_id_async, TokenEntry
from primary.middleware.encrypted_redis_session_store import EncryptedRedisSessionStore

LOGGER = logging.getLogger(__name__)


def create_router_for_acquire_access_token(
    session_store: EncryptedRedisSessionStore,
) -> APIRouter:

    payload_encryption = PayloadEncryption(config.SERVICE_BUS_PAYLOAD_FERNET_KEY)

    router = APIRouter()

    @router.post("/acquire_access_token")
    async def post_acquire_access_token(http_request: Request) -> Response:
        encrypted_body = await http_request.body()

        try:
            token_request = payload_encryption.decrypt_pydantic_model(encrypted_body, AccessTokenRequest, ttl_s=120)
        except (InvalidToken, ValidationError) as exc:
            raise HTTPException(status_code=400, detail="Invalid request") from exc

        dispatch_token_store: DispatchTokenStore = DispatchTokenStoreSingleton.get_instance()
        session_id = await dispatch_token_store.get_session_id_async(token_request.dispatch_token)
        if session_id is None:
            raise HTTPException(status_code=401, detail="Invalid or expired dispatch token")

        token_entry: TokenEntry | None = await get_sumo_access_token_for_session_id_async(
            session_id=session_id, session_store=session_store
        )
        if token_entry is None:
            # Session gone (user logged out) or token cache invalid — treat as auth failure
            raise HTTPException(status_code=401, detail="Could not acquire Sumo token — session may have expired")

        token_response = AccessTokenResponse(
            request_id=token_request.request_id,
            target_service=token_request.target_service,
            access_token=token_entry.token,
            expires_at=token_entry.expires_at,
        )

        encrypted_response_body = payload_encryption.encrypt_pydantic_model(token_response)
        return Response(
            content=encrypted_response_body,
            media_type="application/octet-stream",
            headers={"Cache-Control": "no-store"},
        )

    return router
