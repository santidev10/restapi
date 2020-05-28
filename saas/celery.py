import json
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
from celery import Celery
from utils.celery.logging import init_celery_logging

from kombu import serialization
from kombu.exceptions import DecodeError


def serialize(item):
    return json.dumps(item)


def deserialize(item):
    try:
        data = json.loads(item)
    except TypeError:
        data = item
    return data


serialization.register(
    "celery_result", serialize, deserialize,
    content_type="application/json",
)

app = Celery("saas", task_cls="utils.celery.termination_proof_task:TerminationProofTask")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

init_celery_logging()
