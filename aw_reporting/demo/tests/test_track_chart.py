from datetime import datetime, timedelta
from unittest.mock import patch
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.utittests.patch_now import patch_now
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase


class TrackFiltersAPITestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Track.CHART)

    def setUp(self):
        self.create_test_user()

    def test_success_get(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 1, "one line")
        self.assertEqual(
            len(response.data[0]['data'][0]['trend']), 2, "two days")

    def test_success_dimensions(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
        )
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ('device', 'gender', 'age', 'topic',
                              'interest', 'creative', 'channel', 'video',
                              'keyword', 'location', 'ad'):
                filters['dimension'] = dimension
                url = "{}?{}".format(self.url, urlencode(filters))
                response = self.client.get(url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(len(response.data), 1)
                self.assertGreater(len(response.data[0]['data']), 1)

    def test_success_hourly(self):
        today = now_in_default_tz().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 48, "24 hours x 2 days")

    def test_success_hourly_clicks(self):
        today = now_in_default_tz().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="clicks",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 48, "24 hours x 2 days")
        self.assertEqual(all(i['value'] for i in trend), True)

    def test_success_hourly_today(self):
        now = datetime(2018, 1, 1, 14, 23)
        today = now.date()
        filters = dict(
            start_date=today,
            end_date=today,
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(
            len(trend), now.hour,
            "today's hourly chart "
            "contains only points for the passed hours"
        )

    def test_get_from_future(self):
        today = now_in_default_tz().date()
        filters = dict(
            start_date=today,
            end_date=today + timedelta(days=10),
            indicator="clicks",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 1, "one line")
        self.assertEqual(
            len(response.data[0]['data'][0]['trend']),
            1,
            "Only today's data is present"
        )

    def test_get_from_future_2(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today + timedelta(days=1),
            end_date=today + timedelta(days=10),
            indicator="clicks",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 0,
                         "There is no data from the future")

    def test_get_ctr_v(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=30),
            end_date=today,
            indicator="ctr_v",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertLess(
            max(i['value'] for i in trend),
            10,
            "On real data max CTR(v) is no more than 10%"
        )

    def test_get_ctr(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=30),
            end_date=today,
            indicator="ctr",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertLess(
            max(i['value'] for i in trend),
            5,
            "On real data max CTR is no more than 5%"
        )

    def test_success_on_midnight(self):
        now = datetime(2018, 1, 1, 0, 10)
        today = now.date()
        filters = dict(
            start_date=today,
            end_date=today,
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 0)
