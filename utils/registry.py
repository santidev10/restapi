from __future__ import absolute_import, division, print_function

from contextlib import contextmanager
from threading import local


class Registry:
    _registry = local()

    @classmethod
    def get_request(cls):
        """ returns the request object """
        if cls._registry is not None:
            return getattr(cls._registry, "request", None)

    @classmethod
    def set_request(cls, request):
        """ save the request object """
        setattr(cls._registry, "request", request)

    @classmethod
    def get_user(cls):
        """ returns the current user, if exist, otherwise returns None """
        if cls._registry is not None and hasattr(cls._registry, "user"):
            return getattr(cls._registry, "user")

        request = cls.get_request()
        if request:
            return getattr(request, "user", None)

    @classmethod
    def set_user(cls, user):
        """ save a user as current """
        setattr(cls._registry, "user", user)


class RegistryMiddleware:
    """
    Simple middleware that adds the request object into Registry
    """

    def process_request(self, request):
        Registry.set_request(request)

    def process_response(self, request, response):
        Registry.set_request(None)
        return response


@contextmanager
def current_user(user):
    user_backup = Registry.get_user()
    Registry.set_user(user)
    yield
    Registry.set_user(user_backup)
