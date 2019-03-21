import csv

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordExportTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(
            PathNames.BadWord.EXPORT,
            [Namespace.BRAND_SAFETY],
        )
        return self.client.get(url)

    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Bad Words.csv"
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_response(self):
        self.create_admin_user()
        bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test name",
            category="test category",
        )

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "Id",
            "Name",
            "Category",
        ])
        data = next(csv_data)
        self.assertEqual(data, [str(bad_word.id), bad_word.name, bad_word.category])


def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))
