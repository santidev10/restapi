import os

from celery import Celery

from utils.celery.fix_email_logging import fix_email_logging

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'saas.settings')

app = Celery("saas")
app.config_from_object("django.conf:settings", namespace='CELERY')
app.autodiscover_tasks()

fix_email_logging()
