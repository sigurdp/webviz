import asyncio
import datetime
import json
import logging

import httpx
from azure.servicebus import ServiceBusReceivedMessage

from webviz_server_schemas.pyworker.messages import DevTestMsg

from ..utils import message_decryption
from ..task_exceptions import MalformedMessageError

_logger = logging.getLogger(__name__)


async def dev_test_task_async(sb_msg: ServiceBusReceivedMessage) -> None:
    _logger.info(f"dev_test_task_async(): Parsing DevTestMsg: {sb_msg.message_id=}, {sb_msg.sequence_number=}")

    try:
        body_bytes = b"".join(sb_msg.body)
        msg = DevTestMsg.model_validate_json(body_bytes)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to parse message: {repr(exc)}") from exc

    try:
        decrypted_text = message_decryption.decrypt_data_to_str(msg.encrypted_text)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to decrypt text: {repr(exc)}") from exc

    _logger.info(f"dev_test_task_async(): Message payload: {msg.text=}, {decrypted_text=}")

    _logger.info(f"dev_test_task_async(): Has dispatch token: {"yes" if msg.dispatch_token else "no"}")

    if msg.dispatch_token:
        primary_url = "http://backend-primary:5000/api"

        plaintext = json.dumps({"dispatch_token": msg.dispatch_token}).encode()
        encrypted_request = message_decryption.encrypt_data(plaintext)

        sumo_token: str | None = None
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://backend-primary:5000/api/internal/dispatch_sumo_access_token",
                content=encrypted_request,
                headers={"Content-Type": "application/octet-stream"},
                timeout=10.0,
            )
            response.raise_for_status()

        plaintext_response = message_decryption.decrypt_data_to_bytes(response.content, ttl=120)
        plaintext_dict = json.loads(plaintext_response)

        sumo_token = plaintext_dict["sumo_access_token"]
        expires_at = plaintext_dict["expires_at"]
        expires_at_human = datetime.datetime.fromtimestamp(expires_at, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        _logger.info(f"Got Sumo token for {sb_msg.sequence_number=}, token expires_at={expires_at} ({expires_at_human})")


    # Sleep a bit to simulate work
    _logger.info(f"dev_test_task_async(): Sleeping {msg.sleep_duration_s} seconds to simulate fake processing...")
    await asyncio.sleep(msg.sleep_duration_s)

    if msg.text == "crash":
        _logger.info("dev_test_task_async(): Crashing as requested by message")
        raise RuntimeError("Intentional crash triggered by 'crash' message text")

    _logger.info(f"dev_test_task_async(): Fake processing done {sb_msg.sequence_number=}, had_sumo_token={sumo_token is not None}")



"""
                    # Parse dispatch message and fetch Sumo token from primary
                    try:
                        msg_data = json.loads(body_text)
                        job_id = msg_data["job_id"]
                        dispatch_token = msg_data["dispatch_token"]
                        label = msg_data.get("label", "")
                    except (json.JSONDecodeError, KeyError) as exc:
                        LOGGER.error(f"Malformed message body, cannot parse: {exc}")
                        await receiver.complete_message(msg)
                        continue

                    LOGGER.info(f"Processing job: {job_id=} {label=}")

                    primary_url = os.environ.get("WEBVIZ_PRIMARY_INTERNAL_URL", "http://backend-primary:8080/api")
                    worker_secret = os.environ.get("WEBVIZ_WORKER_CALLBACK_SECRET", "")

                    sumo_token: str | None = None
                    async with httpx.AsyncClient() as http_client:
                        token_resp = await http_client.post(
                            f"{primary_url}/internal/worker/sumo-token",
                            headers={"Authorization": f"Bearer {worker_secret}"},
                            json={"dispatch_token": dispatch_token, "job_id": job_id},
                            timeout=10.0,
                        )
                    if token_resp.status_code == 200:
                        sumo_token = token_resp.json()["sumo_access_token"]
                        expires_at = token_resp.json()["expires_at"]
                        expires_at_human = datetime.datetime.fromtimestamp(expires_at, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                        LOGGER.info(f"Got Sumo token for {job_id=}, token expires_at={expires_at} ({expires_at_human})")
                    else:
                        LOGGER.error(f"Failed to get Sumo token for {job_id=}: HTTP {token_resp.status_code} {token_resp.text}")

                    # Simulate work
                    LOGGER.info(f"Sleeping 5 seconds to simulate work... {job_id=} [{msg.message_id=}, {msg.sequence_number=}]")
                    await asyncio.sleep(5.0)

                    await receiver.complete_message(msg)
                    LOGGER.info(f"Fake processing done {job_id=} [{msg.message_id=}, {msg.sequence_number=}], had_sumo_token={sumo_token is not None}")
"""