import redis

from celery import chord
from celery import group

from django.conf import settings
from saas import celery_app

REDIS_CLIENT = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)


def group_chorded(*signatures):
    return chord(
        group(*signatures),
        empty_callback.si()
    )


@celery_app.task
def empty_callback(*args, **kwargs):
    pass


@celery_app.task(bind=True)
def lock(task, lock_name, **kwargs):
    is_acquired = REDIS_CLIENT.lock(lock_name).acquire(blocking=False)
    if not is_acquired:
        raise task.retry(**kwargs)


@celery_app.task
def unlock(lock_name):
    token = REDIS_CLIENT.get(lock_name)
    REDIS_CLIENT.lock(lock_name).do_release(token)
