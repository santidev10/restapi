from celery import Celery
from django.conf import settings


dmp_celery_app = Celery("update", broker=settings.DMP_BROKER_URL)


class Queue:
    SCHEDULERS = "schedulers"
    CHANNEL_GENERAL_DATA = "channel_general_data"
    CHANNEL_STATS = "channel_stats"
    VIDEO_TRANSCRIPTS = "video_transcripts"
    YOUTUBE_ANALYTICS = "youtube_analytics"
    UPDATE_ADWORDS = "update_adwords"
    DELETE_ENTITY = "delete_entity"


class Task:
    CHANNEL_GENERAL_DATA = "channel_general_data"
    CHANNEL_STATS = "channel_stats"
    DELETE_CHANNEL = "delete_channel"


def send_task_update_channel_stats(task_args):
    dmp_celery_app.send_task(Task.CHANNEL_STATS, task_args, queue=Queue.CHANNEL_STATS)


def send_task_update_channel_general_data(task_args):
    dmp_celery_app.send_task(Task.CHANNEL_GENERAL_DATA, task_args, queue=Queue.CHANNEL_GENERAL_DATA)


def send_task_delete_channel(task_args):
    dmp_celery_app.send_task(Task.DELETE_CHANNEL, task_args, queue=Queue.DELETE_ENTITY)
