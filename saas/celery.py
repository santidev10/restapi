import json
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
from celery import Celery
from utils.celery.logging import init_celery_logging

from kombu import serialization
from kombu.exceptions import DecodeError


def serialize(item):
    return {
        **item,
        "result": json.dumps(item["result"]),
    }


def deserialize(item):
    try:
        result = {
            **item,
            "result": json.loads(item["result"]),
        }
    except DecodeError:
        data = json.loads(item)
        result = {
            **item,
            "result": data["result"]
        }
    return result


serialization.register(
    "celery_result", serialize, deserialize,
    content_type="application/json",
)

app = Celery("saas", task_cls="utils.celery.termination_proof_task:TerminationProofTask")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

init_celery_logging()
