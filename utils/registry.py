from __future__ import absolute_import, division, print_function

from threading import local


class _Registry(local):
    _user = None    # dependency injection from unit tests

    def __init__(self, _user=None):
        super(_Registry, self).__init__()
        self.request = None

    def init(self, request):
        self.request = request

    def reset(self):
        self.request = None

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
