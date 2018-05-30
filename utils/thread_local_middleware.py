from __future__ import absolute_import, division, print_function

from contextlib import contextmanager
from threading import local

_thread_locals = local()


def get_current_request():
    """ returns the request object for this thread """
    return getattr(_thread_locals, "request", None)


def get_current_user():
    """ returns the current user, if exist, otherwise returns None """
    if hasattr(_thread_locals, "user"):
        return getattr(_thread_locals, "user")

    request = get_current_request()
    if request:
        return getattr(request, "user", None)


class ThreadLocalMiddleware:
    """
    Simple middleware that adds the request object in thread local storage.
    """

    def process_request(self, request):
        _thread_locals.request = request

    def process_response(self, request, response):
        if hasattr(_thread_locals, 'request'):
            del _thread_locals.request
        return response


@contextmanager
def current_user(user):
    user_backup = getattr(_thread_locals, "user", None)
    setattr(_thread_locals, "user", user)
    yield
    if user_backup is None:
        delattr(_thread_locals, "user")
    else:
        setattr(_thread_locals, "user", user_backup)
