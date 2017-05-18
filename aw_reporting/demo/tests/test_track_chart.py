from datetime import datetime, timedelta
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase


class TrackFiltersAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.create_test_user()

    def test_success_get(self):
        url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 1, "one line")
        self.assertEqual(
            len(response.data[0]['data'][0]['trend']), 2, "two days")

    def test_success_dimensions(self):
        base_url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
        )
        for dimension in ('device', 'gender', 'age', 'topic', 'interest',
                         # 'creative', 'channel', 'video', TODO: add these tabs when videos and channels are done
                          'keyword', 'location', 'ad'):
            filters['dimension'] = dimension
            url = "{}?{}".format(base_url, urlencode(filters))
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 1)
            self.assertGreater(len(response.data[0]['data']), 1)

    def test_success_hourly(self):
        url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 48, "24 hours x 2 days")
