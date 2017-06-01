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
        url = reverse("aw_reporting_urls:analyze_chart_items",
                      args=(DEMO_ACCOUNT_ID, 'ad'))

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
        self.assertEqual(
            set(data.keys()),
            {'items', 'summary'}
        )
        self.assertEqual(len(data['items']), TOTAL_DEMO_AD_GROUPS_COUNT)
        self.assertEqual(
            set(data['items'][0].keys()),
            {
                'name',
                'video_view_rate',
                'conversions',
                'ctr',
                'status',
                'view_through',
                'all_conversions',
                'average_cpv',
                'video100rate',
                'video_views',
                'video50rate',
                'clicks',
                'average_position',
                'impressions',
                'video75rate',
                'cost',
                'video25rate',
                'average_cpm',
                'ctr_v',
            }
        )

    def test_success_get_filter_items(self):
        url = reverse("aw_reporting_urls:analyze_chart_items",
                      args=(DEMO_ACCOUNT_ID, 'ad'))
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
        self.assertEqual(len(data['items']), len(DEMO_AD_GROUPS))

    def test_demo_all_dimensions(self):
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ('device', 'gender', 'age', 'topic',
                              'interest', 'creative', 'channel', 'video',
                              'keyword', 'location', 'ad'):

                url = reverse("aw_reporting_urls:analyze_chart_items",
                              args=(DEMO_ACCOUNT_ID, dimension))

                response = self.client.post(url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertGreater(len(response.data), 1)

