from pyrabbit.api import Client

from django.conf import settings

VHOST = "restapi"


def get_queue_size(queue_name):
    cl = Client(settings.RABBITMQ_API_URL, settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD)
    return cl.get_queue_depth(VHOST, queue_name)
