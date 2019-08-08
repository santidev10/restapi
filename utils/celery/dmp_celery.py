import logging
from time import sleep
from time import time

from celery import Celery
from celery.backends.base import DisabledBackend

dmp_celery_app = Celery("update")
dmp_celery_app.config_from_object("django.conf:settings", namespace="DMP_CELERY")
logger = logging.getLogger(__name__)

WAIT_RESULTS_TIMEOUT = 20
WAIT_RESULTS_SLEEP_TIME = 1


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


def send_task_channel_general_data_priority(task_args, wait=False):
    future = dmp_celery_app.send_task(
        Task.CHANNEL_GENERAL_DATA_PRIORITY,
        task_args,
        queue=Queue.CHANNEL_GENERAL_DATA_PRIORITY,
    )
    if wait:
        wait_results(future)


def send_task_channel_stats_priority(task_args, wait=False):
    future = dmp_celery_app.send_task(Task.CHANNEL_STATS_PRIORITY, task_args, queue=Queue.CHANNEL_STATS_PRIORITY)
    if wait:
        wait_results(future)


def send_task_delete_channels(task_args):
    dmp_celery_app.send_task(Task.DELETE_CHANNELS, task_args, queue=Queue.DELETE_ENTITY)


def send_task_delete_videos(task_args):
    dmp_celery_app.send_task(Task.DELETE_VIDEOS, task_args, queue=Queue.DELETE_ENTITY)


def wait_results(future):
    if isinstance(future.backend, DisabledBackend):
        logger.warning("Celery backend is disabled. Unable to wait for the results of the async task")
        return
    time_start = time()
    while not future.ready():
        sleep(WAIT_RESULTS_SLEEP_TIME)
        if time() - time_start > WAIT_RESULTS_TIMEOUT:
            break
