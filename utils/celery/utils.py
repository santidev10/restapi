from pyrabbit.api import Client

from django.conf import settings

VHOST = "/"

RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"


def get_queue_size(queue_name):
    cl = Client(settings.RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return cl.get_queue_depth(VHOST, queue_name)