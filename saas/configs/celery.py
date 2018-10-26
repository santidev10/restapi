import os

CELERY_BROKER_HOST = os.getenv("CELERY_BROKER_HOST", "localhost")
CELERY_BROKER_URL = "amqp://{host}".format(host=CELERY_BROKER_HOST)
