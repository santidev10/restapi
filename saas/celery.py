import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
from django.conf import settings


if settings.CELERY_ENABLED:
    from celery import Celery
    from utils.celery.logging import init_celery_logging

    app = Celery("saas", task_cls="utils.celery.termination_proof_task:TerminationProofTask")
    app.config_from_object("django.conf:settings", namespace="CELERY")
    app.control.add_consumer("reports", reply=True)
    app.autodiscover_tasks()

    init_celery_logging()