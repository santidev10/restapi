from celery import Celery
from django.conf import settings


dmp_celery_app = Celery("update", broker=settings.DMP_BROKER_URL)


class Queue:
    SCHEDULERS = "schedulers"
    DELETE_ENTITY = "delete_entity"
    CHANNEL_GENERAL_DATA_PRIORITY = "channel_general_data_priority"
    CHANNEL_STATS_PRIORITY = "channel_stats_priority"


class Task:
    CHANNEL_GENERAL_DATA_PRIORITY = "channel_general_data_priority"
    CHANNEL_STATS_PRIORITY = "channel_stats_priority"
    DELETE_CHANNELS = "delete_channels"
    DELETE_VIDEOS = "delete_videos"


def send_task_channel_general_data_priority(task_args):
    dmp_celery_app.send_task(Task.CHANNEL_GENERAL_DATA_PRIORITY, task_args, queue=Queue.CHANNEL_GENERAL_DATA_PRIORITY)


def send_task_channel_stats_priority(task_args):
    dmp_celery_app.send_task(Task.CHANNEL_STATS_PRIORITY, task_args, queue=Queue.CHANNEL_STATS_PRIORITY)


def send_task_delete_channels(task_args):
    dmp_celery_app.send_task(Task.DELETE_CHANNELS, task_args, queue=Queue.DELETE_ENTITY)


def send_task_delete_videos(task_args):
    dmp_celery_app.send_task(Task.DELETE_VIDEOS, task_args, queue=Queue.DELETE_ENTITY)
