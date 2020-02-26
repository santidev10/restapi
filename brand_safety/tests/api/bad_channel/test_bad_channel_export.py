import csv

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadChannel
from saas.urls.namespaces import Namespace
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BadChannelExportTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(
            PathNames.BadChannel.EXPORT,
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
        expected_filename = "Bad Channels.csv"
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_response(self):
        self.create_admin_user()
        bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            title="test name",
            category="test category",
            youtube_id="testId",
            reason="some reason",
            thumbnail_url="http://example.com/url",
        )

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "Id",
            "Youtube ID",
            "Title",
            "Category",
            "Thumbnail URL",
            "Reason",
        ])
        data = next(csv_data)
        expected_data = [
            str(bad_channel.id),
            bad_channel.youtube_id,
            bad_channel.title,
            bad_channel.category,
            bad_channel.thumbnail_url,
            bad_channel.reason,
        ]
        self.assertEqual(data, expected_data)

    def test_filters_removed(self):
        self.create_admin_user()
        bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            title="test name",
            category="test category",
            youtube_id="testId",
            reason="some reason",
            thumbnail_url="http://example.com/url",
        )
        bad_channel.delete()
        self.assertFalse(BadChannel.objects.all().exists())
        self.assertTrue(BadChannel.objects.get_base_queryset().exists())

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "Id",
            "Youtube ID",
            "Title",
            "Category",
            "Thumbnail URL",
            "Reason",
        ])
        data = [row for row in csv_data]
        self.assertEqual(len(data), 0)
