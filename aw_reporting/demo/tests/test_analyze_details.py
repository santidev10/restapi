from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from saas.utils_tests import ExtendedAPITestCase
from aw_reporting.demo.models import *
import json


class AccountNamesAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.create_test_user()

    def test_success_get_filter_dates(self):
        url = reverse("aw_reporting_urls:analyze_details",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()
        response = self.client.post(
            url,
            json.dumps(dict(start_date=str(today - timedelta(days=2)),
                            end_date=str(today - timedelta(days=1)))),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            {
                "id", "name", 'start_date', 'end_date',
                'age', 'gender', 'device', 'channel', 'creative', 'video',
                'clicks', 'cost', 'impressions', 'video_views',
                'ctr', 'ctr_v', 'average_cpm', 'average_cpv',
                "all_conversions", "conversions", "view_through",
                'video_view_rate', 'average_position', 'ad_network',
                'video100rate', 'video25rate', 'video50rate',
                'video75rate',
            }
        )
        self.assertEqual(data['impressions'], IMPRESSIONS / 10)
        for k in ('age', 'gender', 'device'):
            self.assertGreater(len(data[k]), 1)
            self.assertEqual(
                set(data[k][0].keys()),
                {
                    'name',
                    'value',
                }
            )

    def test_success_get_filter_ad_groups(self):
        url = reverse("aw_reporting_urls:analyze_details",
                      args=(DEMO_ACCOUNT_ID,))
        ad_groups = ["11", "22"]
        response = self.client.post(
            url,
            json.dumps(dict(ad_groups=ad_groups)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            data['impressions'],
            IMPRESSIONS / TOTAL_DEMO_AD_GROUPS_COUNT * len(ad_groups),
        )

