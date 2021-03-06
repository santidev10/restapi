from functools import wraps

import redis
from celery import chord
from celery import group
from django.conf import settings

from saas import celery_app

REDIS_CLIENT = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
DEFAULT_REDIS_LOCK_EXPIRE = 60 * 60 * 6  # 6 hours


def group_chorded(*signatures):
    return chord(
        group(*signatures),
        empty_callback.si()
    )


@celery_app.task
def empty_callback(*args, **kwargs):
    pass


@celery_app.task(bind=True)
def lock(task, lock_name, expire=DEFAULT_REDIS_LOCK_EXPIRE, **kwargs):
    is_acquired = REDIS_CLIENT.lock(lock_name, expire).acquire(blocking=False)
    if not is_acquired:
        raise task.retry(**kwargs)


@celery_app.task
def unlock(lock_name, fail_silently=False):
    try:
        token = REDIS_CLIENT.get(lock_name)
        REDIS_CLIENT.lock(lock_name).do_release(token)
    except BaseException as e:
        if not fail_silently:
            raise e


def celery_lock(lock_key, expire=DEFAULT_REDIS_LOCK_EXPIRE, countdown=60, max_retries=60):
    def _dec(func):
        @wraps(func)
        def _caller(task, *args, **kwargs):
            is_acquired = False
            redis_lock = REDIS_CLIENT.lock(lock_key, expire)
            try:
                is_acquired = redis_lock.acquire(blocking=False)

                if max_retries > 0 and not is_acquired:
                    raise task.retry(countdown=countdown, max_retries=max_retries)

                if is_acquired:
                    result = func(*args, **kwargs)
                else:
                    result = None
            finally:
                if is_acquired:
                    redis_lock.release()
            return result

        return _caller

    return _dec
