import logging
import os

from celery import Celery
from celery.signals import task_failure

from utils.celery.fix_email_logging import fix_email_logging
from utils.exception import wrap_exception

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")

app = Celery("saas")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

fix_email_logging()


@task_failure.connect
def on_task_failure(sender, exception, **kwargs):
    logger = logging.getLogger(sender.name)
    wrapped_error = wrap_exception(kwargs.get("args"), kwargs.get("kwargs"), exception)
    logger.exception(wrapped_error)
