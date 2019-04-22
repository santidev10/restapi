import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
from django.conf import settings

if settings.CELERY_ENABLED:
    from .celery import app as celery_app

    __all__ = ['celery_app']
