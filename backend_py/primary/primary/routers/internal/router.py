import logging
import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from cryptography.fernet import Fernet, InvalidToken
from pydantic_core import ValidationError
from webviz_services.utils.dispatch_token_store import DispatchTokenStore

from primary import config
from primary.auth.auth_helper import get_sumo_access_token_for_session_id

LOGGER = logging.getLogger(__name__)

router = APIRouter()


class _TokenRequest(BaseModel):
    dispatch_token: str


class _TokenResponse(BaseModel):
    sumo_access_token: str
    expires_at: int  # Unix timestamp (seconds since epoch UTC)


def _verify_worker_secret(credentials: HTTPAuthorizationCredentials) -> None:
    """Constant-time comparison to prevent timing attacks."""
    if not secrets.compare_digest(credentials.credentials, config.WORKER_CALLBACK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid worker credentials")


@router.post("/dispatch_sumo_access_token")
async def post_worker_sumo_token(request: Request) -> Response:
    encrypted_body = await request.body()

    fernet = Fernet(config.SERVICE_BUS_PAYLOAD_FERNET_KEY)

    try:
        plaintext = fernet.decrypt(encrypted_body, ttl=120)
        body = _TokenRequest.model_validate_json(plaintext)
    except (InvalidToken, ValidationError):
        raise HTTPException(status_code=400, detail="Invalid request")

    dispatch_token_store: DispatchTokenStore = request.app.state.dispatch_token_store
    session_id = await dispatch_token_store.consume(body.dispatch_token)
    if session_id is None:
        raise HTTPException(status_code=401, detail="Invalid or expired dispatch token")

    token_result = await get_sumo_access_token_for_session_id(
        session_id=session_id,
        session_store=request.app.state.session_store,
    )
    if token_result is None:
        # Session gone (user logged out) or token cache invalid — treat as auth failure
        raise HTTPException(status_code=401, detail="Could not acquire Sumo token — session may have expired")

    sumo_token, expires_at = token_result
    
    plaintext_response = _TokenResponse(sumo_access_token=sumo_token, expires_at=expires_at)

    return Response(
        content=fernet.encrypt(plaintext_response.model_dump_json().encode()),
        media_type="application/octet-stream",
        headers={"Cache-Control": "no-store"},
        )
