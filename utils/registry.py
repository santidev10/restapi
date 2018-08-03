from __future__ import absolute_import, division, print_function

from contextlib import contextmanager
from threading import local


class _Keys:
    USER = "user"
    REQUEST = "request"


class _Registry(local):
    def __init__(self):
        super(_Registry, self).__init__()
        self._user = None
        self.request = None

    def init(self, request, user=None):
        self.request = request
        self._user = user

    def reset(self):
        self.request = None
        self._user = None

    def set_user(self, user):
        self._user = user

    @property
    def user(self):
        if self._user is not None:
            return self._user
        if self.request is not None:
            return self.request.user
        return None


registry = _Registry()


class RegistryMiddleware:
    """
    Simple middleware that adds the request object into Registry
    """

    def process_request(self, request):
        registry.init(request)

    def process_response(self, request, response):
        registry.reset()
        return response


@contextmanager
def current_user(user):
    user_backup = registry.user
    registry.set_user(user)
    yield
    registry.set_user(user_backup)
