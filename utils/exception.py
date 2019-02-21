import logging
from functools import wraps
from time import sleep


class ExceptionWithArgs(Exception):
    def __init__(self, args, kwargs):
        msg = "args={}, kwargs={}".format(args, kwargs)
        super(ExceptionWithArgs, self).__init__(msg)


def wrap_exception(args, kwargs, cause):
    try:
        raise ExceptionWithArgs(args, kwargs) from cause
    except Exception as ex:
        return ex


def ignore_on_error(logger=None, default_result=None):
    if logger is None:
        logger = logging.getLogger("")

    def decorator(fn):

        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                fn(*args, **kwargs)
            except Exception as ex:
                wrapped_error = wrap_exception(args, kwargs, ex)
                logger.exception(wrapped_error)
                return default_result

        return wrapper

    return decorator


def retry(count=3, delay=1, exceptions=(Exception,)):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(count):
                if last_exception is not None:
                    sleep(delay)
                try:
                    return fn(*args, **kwargs)
                except exceptions as ex:
                    last_exception = ex
            raise last_exception

        return wrapper

    return decorator
