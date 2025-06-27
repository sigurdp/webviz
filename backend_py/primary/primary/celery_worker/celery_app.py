from typing import Any
import logging
import os

from celery import Celery
from celery.signals import worker_init, worker_process_init
from dotenv import load_dotenv

from primary.services.utils.task_meta_tracker import TaskMetaTrackerFactory
from primary.services.utils.temp_user_store import TempUserStoreFactory


# Where should these reside? 
# In a config file or grab them from our main config.py?
REDIS_CACHE_URL = "redis://redis-cache:6379"
CELERY_BROKER_URL = "redis://redis-cache:6379/1"
CELERY_RESULT_BACKEND = "redis://redis-cache:6379/2"


# Maybe have this in a separate celeryconfig.py file
celeryconfig = {
    "broker_url": CELERY_BROKER_URL,
    "result_backend": CELERY_RESULT_BACKEND,

    "task_track_started": True,
    # "result_extended": True,
    # "worker_send_task_events": True,
    # "result_persistent ": True,

    "include": [
        "primary.celery_worker.tasks.test_tasks",
        "primary.celery_worker.tasks.surface_tasks",
    ],

    # Celery defaults
    # "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    # "worker_task_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s",
    "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s  [%(name)s]",
    "worker_task_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s  [%(name)s]",

    # Optional: Avoid Celery hijacking the root logger if you want to customize logging manually
    #"worker_hijack_root_logger": False
}


# Register signal handler that runs once in the main process.
# Runs once, in the main worker process, before any forking. Will only ever run one time, regardless of concurrency.
# Should be used to set up things that are/can be shared across all processes.
@worker_init.connect(weak=False)
def on_worker_init(**kwargs: Any) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Doing one-time initialization of Celery main worker process (worker_init)...")

    # Load environment variables from .env file,
    # Note that values set in the system environment will override those in the .env file
    load_dotenv()

    logging.getLogger("primary.celery_worker").setLevel(logging.DEBUG)
    logging.getLogger("celery.task").setLevel(logging.DEBUG)

    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure.monitor.opentelemetry").setLevel(logging.INFO)
    logging.getLogger("azure.monitor.opentelemetry.exporter").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry.attributes").setLevel(logging.ERROR)

    logging.getLogger("httpcore").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("xtgeo").setLevel(logging.WARNING)



# Register signal handler that runs once per worker process, after forking, but before processing any tasks
# Should be used to set up things local to each worker process
@worker_process_init.connect(weak=False)
def on_worker_process_init(**kwargs: Any) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Doing initialization of Celery worker process (worker_process_init)...")

    azure_storage_connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    TempUserStoreFactory.initialize(use_shared_clients=False, redis_url=REDIS_CACHE_URL, storage_account_conn_str=azure_storage_connection_string, ttl_s=2*60)

    TaskMetaTrackerFactory.initialize(redis_url=REDIS_CACHE_URL, ttl_s=2*60)

    if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry.instrumentation.celery import CeleryInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        logger.info("Configuring Azure Monitor telemetry for celery worker")

        configure_azure_monitor(logging_formatter=logging.Formatter("[%(name)s]: %(message)s"))

        CeleryInstrumentor().instrument()
        HTTPXClientInstrumentor().instrument()
    else:
        logger.warning("Skipping telemetry configuration for celery worker, APPLICATIONINSIGHTS_CONNECTION_STRING env variable not set.")


celery_app = Celery("worker")
celery_app.config_from_object(celeryconfig)


