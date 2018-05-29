from __future__ import absolute_import, division, print_function

from contextlib import contextmanager


class Registry:
    _registry = None

    @classmethod
    def reset(cls):
        cls._registry = None

    @classmethod
    def init(cls):
        Registry._registry = dict()

    @classmethod
    def get_request(cls):
        """ returns the request object """
        if cls._registry is not None:
            return Registry._registry.get("request")

    @classmethod
    def set_request(cls, request):
        """ save the request object """
        cls._registry["request"] = request

    @classmethod
    def get_user(cls):
        """ returns the current user, if exist, otherwise returns None """
        if cls._registry is not None and "user" in cls._registry:
            return cls._registry.get("user")

        request = cls.get_request()
        if request:
            return getattr(request, "user", None)

    @classmethod
    def set_user(cls, user):
        """ save a user as current """
        cls._registry["user"] = user


class RegistryMiddleware:
    """
    Simple middleware that adds the request object into Registry
    """

    def process_request(self, request):
        Registry.init()
        Registry.set_request(request)

    def process_response(self, request, response):
        Registry.reset()
        return response

@contextmanager
def current_user(user):
    user_backup = Registry.get_user()
    Registry.set_user(user)
    yield
    Registry.set_user(user_backup)
