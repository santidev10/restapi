from pyrabbit.api import Client

from django.conf import settings

VHOST = '/'


def get_queue_size(queue_name):
    rabbitmq_api_url = "{}:{}".format(settings.CELERY_BROKER_HOST, settings.CELERY_BROKER_PORT)
    cl = Client("rabbitmq:15672", 'guest', 'guest')
    cl = Client(rabbitmq_api_url, settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD)
    return cl.get_queue_depth(VHOST, queue_name)