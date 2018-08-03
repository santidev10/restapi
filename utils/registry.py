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

    @property
    def user(self):
        if self._user is not None:
            return self._user
        if self.request is not None:
            return self.request.user
        return None

    @user.setter
    def user(self, value):
        self._user = value


registry = _Registry()


class RegistryMiddleware:
    """
    Simple middleware that adds the request object into Registry
    """

    def process_request(self, request):
        registry.request = request
        registry.user = request.user

    def process_response(self, request, response):
        if registry.request is not None and registry.request is not request:
            raise Exception("Registry has been corrupted")
        if registry._user is not None and registry._user is not request.user:
            raise Exception("Registry.user has been corrupted")
        registry.request = None
        registry.user = None
        return response


@contextmanager
def current_user(user):
    user_backup = registry.user
    registry.user = user
    yield
    registry.user = user_backup
