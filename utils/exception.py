from functools import wraps
from random import randint
from time import sleep


class ExceptionWithArgs(Exception):
    def __init__(self, args, kwargs):
        msg = "args={}, kwargs={}".format(args, kwargs)
        super(ExceptionWithArgs, self).__init__(msg)


def wrap_exception(args, kwargs, cause):
    try:
        raise ExceptionWithArgs(args, kwargs) from cause
    except BaseException as ex:
        return ex


def retry(count=3, delay=1, exceptions=(Exception,)):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exception = None
            for _ in range(count):
                if last_exception is not None:
                    sleep(delay)
                try:
                    return fn(*args, **kwargs)
                except exceptions as ex:
                    last_exception = ex
            raise last_exception

        return wrapper

    return decorator


def backoff(max_backoff: int = 3600, exceptions: tuple = (Exception,)):
    """
    :param max_backoff: Max seconds to back off before raising last exception
    :param exceptions: tuple of exceptions to catch
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            step = 0
            slept = 0
            errors = None
            while slept <= max_backoff:
                try:
                    res = func(*args, **kwargs)
                except exceptions as e:
                    errors = e
                    sleeping = (2**step + randint(0, 1000)) / 1000
                    sleep(sleeping)
                    step += 1
                    slept += sleeping
                else:
                    return res
            raise errors
        return wrapper
    return decorator
