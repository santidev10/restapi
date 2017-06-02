import json
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.demo.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class AccountNamesAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.create_test_user()

    def test_success_get_filter_dates(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(DEMO_ACCOUNT_ID,))

        today = datetime.now().date()
        response = self.client.post(
            url,
            json.dumps(dict(
                start_date=str(today - timedelta(days=2)),
                end_date=str(today - timedelta(days=1)),
                indicator="impressions",
            )),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(data[1]['title'], "Campaign #demo1")
        self.assertEqual(data[2]['title'], "Campaign #demo2")

    def test_success_get_filter_items(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()
        start_date = str(today - timedelta(days=2))
        end_date = str(today - timedelta(days=1))
        response = self.client.post(
            url,
            json.dumps(dict(start_date=start_date,
                            end_date=end_date,
                            campaigns=["demo1"])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], "Campaign #demo1")

        self.assertEqual(
            set(str(i['label']) for i in data[0]['data'][0]['trend']),
            {start_date, end_date},
        )

    def test_demo_all_dimensions(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()

        filters = {
            'indicator': 'video_view_rate',
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ('device', 'gender', 'age', 'topic',
                              'interest', 'creative', 'channel', 'video',
                              'keyword', 'location', 'ad'):

                filters['dimension'] = dimension
                response = self.client.post(
                    url, json.dumps(filters),
                    content_type='application/json',
                )
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(len(response.data), 3)
                self.assertGreater(len(response.data[0]['data']), 1)

