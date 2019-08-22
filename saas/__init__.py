import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
from .celery import app as celery_app
from es_components.connections import init_es_connection

init_es_connection()
__all__ = ['celery_app']
