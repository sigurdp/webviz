import logging

from primary.celery_worker.celery_app import celery_app

LOGGER = logging.getLogger(__name__)


@celery_app.task
def process_data(data: dict):
    LOGGER.info(f"Processing data --- : {data}")
    return f"Processed: {data['item']}"

