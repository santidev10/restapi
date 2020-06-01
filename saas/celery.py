import json
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
# pylint: disable=wrong-import-position
from celery import Celery
from kombu import serialization

from utils.celery.logging import init_celery_logging


# pylint: enable=wrong-import-position


def serialize(item):
    return {
        **item,
        "result": json.dumps(item["result"]),
    }


def deserialize(item):
    return {
        **item,
        "result": json.loads(item["result"]),
    }


serialization.register(
    "celery_result", serialize, deserialize,
    content_type="application/celery-result",
)

app = Celery("saas", task_cls="utils.celery.termination_proof_task:TerminationProofTask")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

init_celery_logging()
