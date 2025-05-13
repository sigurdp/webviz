import logging
import os


from dotenv import load_dotenv
#from celery import Celery
from celery.signals import worker_process_init, setup_logging

from primary import config

from primary.celery_worker.celery_app import celery_app


logging.basicConfig(format="%(asctime)s %(levelname)-7s [%(name)s]: %(message)s", datefmt="%H:%M:%S")

logging.getLogger("primary").setLevel(logging.DEBUG)

logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.monitor.opentelemetry").setLevel(logging.INFO)
logging.getLogger("azure.monitor.opentelemetry.exporter").setLevel(logging.WARNING)

logging.getLogger("httpcore").setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.INFO)
logging.getLogger("xtgeo").setLevel(logging.INFO)

LOGGER = logging.getLogger(__name__)


# Load environment variables from .env file,
# Note that values set in the system environment will override those in the .env file
load_dotenv()


@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    LOGGER.info("Entering init_celery_tracing()")

    if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry.instrumentation.celery import CeleryInstrumentor

        LOGGER.info("Configuring Azure Monitor telemetry for celery worker")

        configure_azure_monitor(logging_formatter=logging.Formatter("[%(name)s]: %(message)s"))

        CeleryInstrumentor().instrument()
    else:
        LOGGER.warning("Skipping telemetry configuration for celery worker, APPLICATIONINSIGHTS_CONNECTION_STRING env variable not set.")



