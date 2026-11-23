import asyncio
import datetime
import json
import logging
import uuid

import httpx
from azure.servicebus import ServiceBusReceivedMessage

from webviz_server_schemas.internal.acquire_access_token import AccessTokenRequest, AccessTokenResponse
from webviz_server_schemas.pyworker.messages import DevTestMsg

from ..utils.payload_encryption_singleton import PayloadEncryptionSingleton, PayloadEncryption
from ..task_exceptions import MalformedMessageError

_logger = logging.getLogger(__name__)


async def dev_test_task_async(sb_msg: ServiceBusReceivedMessage) -> None:
    payload_encryption: PayloadEncryption = PayloadEncryptionSingleton.get_instance()

    _logger.info(f"dev_test_task_async(): Parsing DevTestMsg: {sb_msg.message_id=}, {sb_msg.sequence_number=}")

    try:
        body_bytes = b"".join(sb_msg.body)
        msg = DevTestMsg.model_validate_json(body_bytes)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to parse message: {repr(exc)}") from exc

    try:
        decrypted_text = payload_encryption.decrypt_to_str(msg.encrypted_text)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to decrypt text: {repr(exc)}") from exc

    _logger.info(f"dev_test_task_async(): Message payload: {msg.text=}, {decrypted_text=}")

    _logger.info(f"dev_test_task_async(): Has dispatch token: {"yes" if msg.dispatch_token else "no"}")

    sumo_token: str | None = None
    if msg.dispatch_token:
        sumo_token, expires_at = await fetch_sumo_access_token_async(payload_encryption, msg.dispatch_token)
        expires_at_human = datetime.datetime.fromtimestamp(expires_at, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        current_time_human = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        _logger.info(f"Got Sumo token for {sb_msg.sequence_number=}, token expires_at={expires_at} ({expires_at_human}, current time={current_time_human})")


    # Sleep a bit to simulate work
    _logger.info(f"dev_test_task_async(): Sleeping {msg.sleep_duration_s} seconds to simulate fake processing...")
    await asyncio.sleep(msg.sleep_duration_s)

    if msg.text == "crash":
        _logger.info("dev_test_task_async(): Crashing as requested by message")
        raise RuntimeError("Intentional crash triggered by 'crash' message text")

    _logger.info(f"dev_test_task_async(): Fake processing done {sb_msg.sequence_number=}, had_sumo_token={sumo_token is not None}")


async def fetch_sumo_access_token_async(payload_encryption: PayloadEncryption, dispatch_token: str) -> tuple[str, int]:
    token_request = AccessTokenRequest(
        request_id=uuid.uuid4(),
        dispatch_token=dispatch_token,
        target_service="sumo",
    )

    encrypted_request_body = payload_encryption.encrypt_pydantic_model(token_request)

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://backend-primary:5000/api/internal/acquire_access_token",
            content=encrypted_request_body,
            headers={"Content-Type": "application/octet-stream"},
            timeout=10.0,
        )
        response.raise_for_status()

    token_response = payload_encryption.decrypt_pydantic_model(response.content, AccessTokenResponse, ttl_s=120)

    if token_response.request_id != token_request.request_id:
        raise RuntimeError("Mismatched request ID in token response")
    if token_response.target_service != "sumo":
        raise RuntimeError("Unexpected target service in token response")

    return token_response.access_token, token_response.expires_at

