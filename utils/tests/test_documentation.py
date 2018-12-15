from unittest import skip

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from utils.documentation import PathName
from utils.utittests.generic_test import generic_test
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class SchemaFormat:
    JSON = ".json"
    YAML = ".yaml"


TEST_ARGS = [
    (PathName.SWAGGER, ()),
    (PathName.REDOC, ()),
    (PathName.SCHEMA, (SchemaFormat.JSON,)),
    (PathName.SCHEMA, (SchemaFormat.YAML,)),
]


class DocumentationApiTestCase(ExtendedAPITestCase):
    def _request(self, path_name, args):
        url = reverse(path_name, [], args=args)
        return self.client.get(url)

    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_unauthorized(self, path_name, args):
        response = self._request(path_name, args)

        # todo: should be HTTP_401_UNAUTHORIZED, not HTTP_403_FORBIDDEN
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_forbidden(self, path_name, args):
        self.create_test_user()
        response = self._request(path_name, args)

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @skip("Can't login user")
    @generic_test([
        (None, args, dict())
        for args in TEST_ARGS
    ])
    def test_success(self, path_name, args):
        self.create_admin_user()
        response = self._request(path_name, args)

        self.assertEqual(response.status_code, HTTP_200_OK)
