import os
import sys
import asyncio
import logging

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.sdk.resources import Resource
from opentelemetry.propagate import extract
from opentelemetry.propagate import inject
from opentelemetry import trace

from webviz_core_utils.radix_utils import is_running_on_radix_platform
from webviz_core_utils.azure_monitor_destination import AzureMonitorDestination

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver
from azure.servicebus import ServiceBusReceivedMessage



logging.basicConfig(format="%(asctime)s %(levelname)-7s [%(name)s]: %(message)s", datefmt="%H:%M:%S")
logging.getLogger().setLevel(logging.INFO)

LOGGER = logging.getLogger(__name__)



def _setup_azure_monitor_telemetry_for_mock_worker() -> None:

    LOGGER.info("Configuring Azure Monitor telemetry for worker-mock...")

    # Due to our default log level of DEBUG, these loggers become quite noisy, so limit them to INFO or WARNING
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure.monitor.opentelemetry").setLevel(logging.INFO)
    logging.getLogger("azure.monitor.opentelemetry.exporter").setLevel(logging.WARNING)

    if is_running_on_radix_platform():
        azmon_dest = AzureMonitorDestination.from_radix_env()
    else:
        # Picks up APPLICATIONINSIGHTS_CONNECTION_STRING env variable if it is set, and configures from that.
        azmon_dest = AzureMonitorDestination.for_local_dev(service_name="worker-mock")

    if not azmon_dest:
        LOGGER.warning("Skipping telemetry configuration for worker-mock, no valid AzureMonitorDestination")
        return

    LOGGER.info(
        f"Configuring Azure Monitor telemetry for worker-mock, resource attributes: {azmon_dest.resource_attributes}"
    )

    configure_azure_monitor(
        connection_string=azmon_dest.insights_connection_string,
        resource=Resource.create(attributes=azmon_dest.resource_attributes),
        sampling_ratio=1.0,
        logging_formatter=logging.Formatter("[%(name)s]: %(message)s"),
    )






def _normalize_sb_props(props: dict | None) -> dict[str, str]:
    if not props:
        return {}
    norm: dict[str, str] = {}
    for k, v in props.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        norm[kk.lower()] = vv
    # Some Azure SDKs set only "Diagnostic-Id"; map it to W3C if "traceparent" is missing
    if "traceparent" not in norm and "diagnostic-id" in norm:
        norm["traceparent"] = norm["diagnostic-id"]
    return norm




async def main_async() -> int:
    LOGGER.info("Starting worker...")
    LOGGER.info("========================================")

    _setup_azure_monitor_telemetry_for_mock_worker()

    sb_conn_string = os.getenv("SERVICEBUS_CONNECTION_STRING")
    LOGGER.info(f"{sb_conn_string=}")

    if sb_conn_string:
        LOGGER.info("Using SERVICEBUS_CONNECTION_STRING from environment")
        sb_client = ServiceBusClient.from_connection_string(conn_str=sb_conn_string, retry_total=10)
    else:
        LOGGER.info("Using DefaultAzureCredential for authentication")
        LOGGER.info(f"AZURE_TENANT_ID: {os.getenv('AZURE_TENANT_ID')}")
        LOGGER.info(f"AZURE_CLIENT_ID: {os.getenv('AZURE_CLIENT_ID')}")

        # Relies on AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET being set in environment
        credential = DefaultAzureCredential()
        LOGGER.info(f"{type(credential)=}")

        fully_qualified_sb_namespace = "webviz-test.servicebus.windows.net"
        sb_client = ServiceBusClient(fully_qualified_namespace=fully_qualified_sb_namespace, credential=credential)

    queue_name = "test-queue"
    LOGGER.info(f"{queue_name=}")

    async with sb_client:
        receiver: ServiceBusReceiver = sb_client.get_queue_receiver(
            queue_name=queue_name,
            max_wait_time=None  # seconds to wait for messages before returning
        )

        async with receiver:
            LOGGER.info("---------------")
            LOGGER.info("Waiting for messages...")
            LOGGER.info("---------------")

            msg: ServiceBusReceivedMessage
            async for msg in receiver:
                # LOGGER.info(f"Got message repr: {repr(msg)=}")
                
                # LOGGER.info(f"{type(msg.application_properties)=}")
                LOGGER.info(f"{msg.application_properties=}")

                props = getattr(msg, "application_properties", {})  # could be bytes
                carrier = _normalize_sb_props(props)
                # LOGGER.info(f"{carrier=}")

                parent_ctx = extract(carrier)
                # LOGGER.info(f"Got message parent context: {parent_ctx=}")

                tracer = trace.get_tracer(__name__)
                with tracer.start_as_current_span("process_my_message", context=parent_ctx):

                    body_bytes = b"".join(msg.body)
                    body_text = body_bytes.decode("utf-8")
                    LOGGER.info(f"Got message: [{msg.message_id=}, {msg.sequence_number=}]: {body_text=}")

                    # Sleep a bit to simulate work
                    LOGGER.info(f"Sleeping 5 seconds to simulate work... [{msg.message_id=}, {msg.sequence_number=}]: {body_text=}")
                    await asyncio.sleep(5.0)

                    if body_text == "exit":
                        LOGGER.info("Exiting as requested by message")
                        await receiver.complete_message(msg)
                        return 0

                    if body_text == "crash":
                        LOGGER.info("Crashing as requested by message")
                        await receiver.complete_message(msg)
                        raise RuntimeError("Intentional crash triggered by 'crash' message")

                    await receiver.complete_message(msg)
                    LOGGER.info(f"Fake processing done [{msg.message_id=}, {msg.sequence_number=}]: {body_text=}")

            LOGGER.info("No more messages (timeout).")



# !!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!
# https://chatgpt.com/c/68dfbb72-2c78-8325-88f6-f86f68ae51bc


if __name__ == "__main__":
    LOGGER.info("Entering script ...")

    try:
        exit_code = asyncio.run(main_async())
    except Exception:
        # A last-defense guard in case something escapes even higher
        LOGGER.exception("Fatal error outside asyncio.run")
        exit_code = 1

    if not isinstance(exit_code, int):
        exit_code = 1
        
    LOGGER.info(f"Exiting script with code: {exit_code}")

    sys.exit(exit_code if isinstance(exit_code, int) else 1)