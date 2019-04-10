import os

from celery.schedules import crontab

CELERY_BROKER_HOST = os.getenv("CELERY_BROKER_HOST", "localhost")
CELERY_BROKER_PORT = os.getenv("CELERY_BROKER_PORT", "15672")
CELERY_BROKER_URL = "amqp://{host}".format(host=CELERY_BROKER_HOST)
CELERY_TIMEZONE = "UTC"
CELERY_BEAT_SCHEDULE = {
    "full-aw-update": {
        "task": "aw_reporting.update.update_aw_accounts.update_aw_accounts",
        "schedule": crontab(hour="5,13,21", minute="0"),  # each 8 hours including 6AM in LA
    },
    "update-audiences": {
        "task": "aw_reporting.update.tasks.update_audiences.update_audiences_from_aw",
        "schedule": crontab(day_of_month="1", hour="0", minute="0"),
    },
}
CELERY_RESULT_BACKEND = "django-db"
RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"
