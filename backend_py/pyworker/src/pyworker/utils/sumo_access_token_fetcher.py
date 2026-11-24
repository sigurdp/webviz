import uuid

import httpx
from cryptography.fernet import InvalidToken
from pydantic import ValidationError

from webviz_server_schemas.internal.acquire_access_token import AccessTokenRequest, AccessTokenResponse
from webviz_services.utils.httpx_async_client_wrapper import HTTPX_ASYNC_CLIENT_WRAPPER
from webviz_services.utils.payload_encryption import PayloadEncryption

from ..task_exceptions import TaskFailedError, TaskInternalError, TaskRetryError


class SumoAccessTokenFetcher:
    def __init__(self, primary_backend_url: str, payload_encryption: PayloadEncryption) -> None:
        self._endpoint = f"{primary_backend_url.rstrip('/')}/api/internal/acquire_access_token"
        self._payload_encryption = payload_encryption

    async def fetch_token_async(self, dispatch_token: str) -> tuple[str, int]:
        token_request = AccessTokenRequest(
            request_id=uuid.uuid4(),
            dispatch_token=dispatch_token,
            target_service="sumo",
        )
        encrypted_request_body = self._payload_encryption.encrypt_pydantic_model(token_request)

        try:
            response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
                self._endpoint,
                content=encrypted_request_body,
                headers={"Content-Type": "application/octet-stream"},
                timeout=10.0,
            )
            response.raise_for_status()

        except httpx.RequestError as exc:
            raise TaskRetryError("Could not reach the internal service for acquiring access token") from exc
        except httpx.HTTPStatusError as exc:
            http_status = exc.response.status_code
            if http_status == 401:
                raise TaskFailedError("Task authorization expired. Please sign in and submit the task again.") from exc

            raise TaskInternalError(f"Internal token service returned HTTP {http_status}") from exc

        try:
            # The TTL here ensures that the decrypted response is not older than 120 seconds.
            token_response = self._payload_encryption.decrypt_pydantic_model(
                response.content, AccessTokenResponse, ttl_s=120
            )
        except (InvalidToken, ValidationError) as exc:
            raise TaskInternalError("Got an invalid or malformed response from internal token service") from exc

        if token_response.request_id != token_request.request_id:
            raise TaskInternalError("Mismatched request ID in token response")
        if token_response.target_service != "sumo":
            raise TaskInternalError("Unexpected target service in token response")

        return token_response.access_token, token_response.expires_at
