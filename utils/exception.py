from functools import wraps
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
