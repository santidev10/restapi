from celery import chord
from celery import group

from saas import celery_app
from utils.filelock import FileLock


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
    try:
        FileLock(lock_name).acquire()
    except FileExistsError:
        raise task.retry(**kwargs)


@celery_app.task
def unlock(lock_name):
    FileLock(lock_name).release()
