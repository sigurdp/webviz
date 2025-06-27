import logging

from primary.celery_worker.celery_app import celery_app


@celery_app.task
def capitalize_string(input_str: str) -> str:
    logger = logging.getLogger(__name__)
    logger.info(f"capitalize_string --- : {input_str=}")

    ret_str = input_str
    ret_str.capitalize()

    return f"Processed: {ret_str=}"


