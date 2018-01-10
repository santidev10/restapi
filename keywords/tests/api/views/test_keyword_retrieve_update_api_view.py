from unittest.mock import patch, ANY
from urllib.parse import quote_plus

import requests
from rest_framework.reverse import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase, MockResponse


class PathEndsWith(object):
    def __init__(self, path):
        self.expected_path = path

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        return other.split("?")[0].endswith(self.expected_path)

    def __repr__(self):
        return "Urls path ends with {path}".format(path=self.expected_path)


class KeywordRetrieveUpdateApiViewTestCase(ExtendedAPITestCase):
    @patch.object(requests, "get")
    def test_get_keyword_should_decode_pk_for_sdb_call(self, get_mock):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1807
        Description: Opening keyword with special symbols leads to 408 error
        Root cause: keyword should be decoded before lookup
        """
        keyword = "#tigerzindahai"
        self.assertNotEqual(keyword, quote_plus(keyword),
                            "Test does not make sense")

        self.create_test_user()
        get_mock.return_value = MockResponse(json={"keyword": keyword})
        url = reverse("keyword_api_urls:keywords", args=(keyword,))
        response = self.client.get(url)

        path_suffix = "keywords/{keyword}/".format(keyword=quote_plus(keyword))

        get_mock.assert_called_once_with(PathEndsWith(path_suffix),
                                         headers=ANY, verify=ANY)
        self.assertEqual(response.status_code, HTTP_200_OK)
