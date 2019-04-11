import os

from celery.schedules import crontab


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_API_PORT = os.getenv("RABBITMQ_API_PORT", 15672)
RABBITMQ_AMQP_PORT = os.getenv("RABBITMQ_AMQP_PORT", 5672)

RABBITMQ_API_URL = "{host}:{port}".format(host=RABBITMQ_HOST, port=RABBITMQ_API_PORT)
CELERY_BROKER_URL = "amqp://{host}:{port}".format(host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)

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
