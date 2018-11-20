import json

from rest_framework.status import HTTP_200_OK


class MockResponse(object):
    def __init__(self, status_code=HTTP_200_OK, **kwargs):
        self.status_code = status_code
        self._json = kwargs.pop("json", None)

    def json(self):
        return self._json or dict()

    @property
    def text(self):
        return json.dumps(self.json())
