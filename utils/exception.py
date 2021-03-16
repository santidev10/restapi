from functools import wraps
from random import randint
from time import sleep

from elasticsearch.helpers.errors import BulkIndexError


class ExceptionWithArgs(Exception):
    def __init__(self, args, kwargs):
        msg = "args={}, kwargs={}".format(args, kwargs)
        super(ExceptionWithArgs, self).__init__(msg)


def wrap_exception(args, kwargs, cause):
    try:
        raise ExceptionWithArgs(args, kwargs) from cause
    except BaseException as ex:
        return ex


def retry(count=3, delay=1, exceptions=(Exception,), failed_callback=None, failed_kwargs=None):
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
            if failed_callback is not None:
                all_kwargs = {**kwargs, **(failed_kwargs or {})}
                failed_callback(*args, **all_kwargs)
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


def upsert_retry(manager, docs: list, max_tries=5, delay=2, **params) -> None:
    """
    Function to retry bulk upsert documents in case of BulkIndexError
        Not all documents given to upsert will fail, so extract failed doc ids from exception and retry
        with only failed docs
    :param manager: VideoManager | ChannelManager
    :param docs: list of documents to upsert
    :param max_tries: Max number of tries
    :param delay: Time to sleep between tries
    :param params: kwargs to pass to manager.upsert method
    :return: None
    """
    for _ in range(max_tries):
        if not docs:
            return
        try:
            manager.upsert(docs, **params)
        except BulkIndexError as e:
            sleep(delay)
            doc_ids = {err["update"]["_id"] for err in e.errors}
            docs = [doc for doc in docs if doc.main.id in doc_ids]
        else:
            docs.clear()
