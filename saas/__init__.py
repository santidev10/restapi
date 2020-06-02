import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
# pylint: disable=wrong-import-position
from es_components.connections import init_es_connection
from .celery import app as celery_app

# pylint: enable=wrong-import-position

init_es_connection()
__all__ = ["celery_app"]
