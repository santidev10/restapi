import os

from celery import chord, group
from django.conf import settings

from saas import celery_app


def group_chorded(*signatures):
    return chord(
        group(*signatures),
        empty_callback.si()
    )


@celery_app.task
def empty_callback(*args, **kwargs):
    pass


def get_lock_path(lock_name):
    return os.path.join(settings.BASE_DIR, "locks", lock_name)


@celery_app.task(bind=True)
def lock(task, lock_name, **kwargs):
    lock_path = get_lock_path(lock_name)
    if os.path.exists(lock_path):
        raise task.retry(**kwargs)
    else:
        f = open(lock_path, "a")
        f.close()


@celery_app.task
def unlock(lock_name):
    lock_path = get_lock_path(lock_name)
    if os.path.exists(lock_path):
        os.remove(lock_path)
