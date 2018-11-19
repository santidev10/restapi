import logging
from functools import wraps


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
