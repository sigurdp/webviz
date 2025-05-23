from celery import Celery

from primary import config


# Maybe have this in a separate celeryconfig.py file
celeryconfig = {
    "broker_url": config.CELERY_BROKER_URL,
    "result_backend": config.CELERY_RESULT_BACKEND,
    
    "task_track_started": True,
    # "result_extended": True,
    # "worker_send_task_events": True,
    # "result_persistent ": True,

    # Celery defaults
    # "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    # "worker_task_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s",
    
    "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s  [%(name)s]",
    "worker_task_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s  [%(name)s]",

    # Optional: Avoid Celery hijacking the root logger if you want to customize logging manually
    #"worker_hijack_root_logger": False
}


celery_app = Celery("worker")

# Could also be a module
celery_app.config_from_object(celeryconfig)

celery_app.autodiscover_tasks(["primary.celery_worker.tasks.test_tasks"])
