from celery import Celery

from primary import config


celery_app = Celery("worker", broker=config.CELERY_BROKER_URL, backend=config.CELERY_RESULT_BACKEND)

celery_app.autodiscover_tasks(["primary.celery_worker.tasks.test_tasks"])
