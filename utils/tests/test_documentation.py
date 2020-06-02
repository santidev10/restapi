from django.test import RequestFactory
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from utils.documentation import PathName
from utils.documentation import schema_view
from utils.unittests.generic_test import generic_test
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class SchemaFormat:
    JSON = ".json"
    YAML = ".yaml"


TEST_ARGS = [
    (PathName.SWAGGER, (), schema_view.with_ui(PathName.SWAGGER, cache_timeout=0)),
    (PathName.REDOC, (), schema_view.with_ui(PathName.SWAGGER, cache_timeout=0)),
    (PathName.SCHEMA, (SchemaFormat.JSON,), schema_view.without_ui(cache_timeout=0)),
    (PathName.SCHEMA, (SchemaFormat.YAML,), schema_view.without_ui(cache_timeout=0)),
]


class DocumentationApiTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.factory = RequestFactory()

    def _request(self, path_name, args, view, user=None):
        url = reverse(path_name, [Namespace.DOCUMENTATION], args=args)
        request = self.factory.get(url)
        if user is not None:
            request.user = user
        return view(request)

    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_unauthorized(self, path_name, args, view):
        response = self._request(path_name, args, view)

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_forbidden(self, path_name, args, view):
        user = self.create_test_user()
        response = self._request(path_name, args, view, user)

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_success(self, path_name, args, view):
        user = self.create_admin_user()

        response = self._request(path_name, args, view, user)

        self.assertEqual(response.status_code, HTTP_200_OK)
