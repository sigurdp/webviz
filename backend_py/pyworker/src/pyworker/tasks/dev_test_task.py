import asyncio
import datetime
import logging
from azure.servicebus import ServiceBusReceivedMessage

from webviz_server_schemas.pyworker.messages import DevTestMsg

from ..task_services import TaskServices
from ..task_exceptions import MalformedMessageError

_logger = logging.getLogger(__name__)


async def dev_test_task_async(sb_msg: ServiceBusReceivedMessage, services: TaskServices) -> None:
    _logger.info(f"dev_test_task_async(): Parsing DevTestMsg: {sb_msg.message_id=}, {sb_msg.sequence_number=}")

    try:
        body_bytes = b"".join(sb_msg.body)
        msg = DevTestMsg.model_validate_json(body_bytes)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to parse message: {repr(exc)}") from exc

    try:
        decrypted_text = services.payload_encryption.decrypt_to_str(msg.encrypted_text)
    except Exception as exc:
        raise MalformedMessageError(f"Failed to decrypt text: {repr(exc)}") from exc

    _logger.info(f"dev_test_task_async(): Message payload: {msg.text=}, {decrypted_text=}")

    _logger.info(f"dev_test_task_async(): Has dispatch token: {"yes" if msg.dispatch_token else "no"}")

    sumo_token: str | None = None
    if msg.dispatch_token:
        sumo_token, expires_at = await services.sumo_access_token_fetcher.fetch_token_async(msg.dispatch_token)

        fmt = "%Y-%m-%d %H:%M:%S UTC"
        expires_at_human = datetime.datetime.fromtimestamp(expires_at, tz=datetime.timezone.utc).strftime(fmt)
        current_time_human = datetime.datetime.now(datetime.timezone.utc).strftime(fmt)
        _logger.info(
            f"Got Sumo token for {sb_msg.sequence_number=}, token expires_at={expires_at} ({expires_at_human}, current time={current_time_human})"
        )

    # Sleep a bit to simulate work
    _logger.info(f"dev_test_task_async(): Sleeping {msg.sleep_duration_s} seconds to simulate fake processing...")
    await asyncio.sleep(msg.sleep_duration_s)

    if msg.text == "crash":
        _logger.info("dev_test_task_async(): Crashing as requested by message")
        raise RuntimeError("Intentional crash triggered by 'crash' message text")

    _logger.info(
        f"dev_test_task_async(): Fake processing done {sb_msg.sequence_number=}, had_sumo_token={sumo_token is not None}"
    )
