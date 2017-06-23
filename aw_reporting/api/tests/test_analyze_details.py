import json
from unittest.mock import patch
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from .base import AwReportingAPITestCase


class AccountNamesAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.create_test_user()

    def test_success_get_filter_dates_demo(self):
        url = reverse("aw_reporting_urls:analyze_details",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
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
                'video75rate', 'video_views_this_week',
                'video_view_rate_top', 'impressions_this_week',
                'video_views_lask_week', 'cost_this_week',
                'video_view_rate_bottom', 'clicks_this_week',
                'ctr_v_top', 'cost_last_week', 'average_cpv_top',
                'ctr_v_bottom', 'ctr_bottom', 'click_last_weel',
                'average_cpv_bottom', 'ctr_top', 'impressions_last_week'
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

        for k in ('channel', 'creative', 'video'):
            self.assertEqual(len(data[k]), 3)
            self.assertEqual(
                set(data[k][0].keys()),
                {
                    'id',
                    'name',
                    'thumbnail',
                    'impressions',
                    'video_views',
                    'ctr_v',
                    'average_cpv',
                    'average_cpm',
                    'cost',
                    'clicks',
                    'ctr',
                    'video_view_rate',
                }
            )

    def test_success_get_filter_ad_groups_demo(self):
        url = reverse("aw_reporting_urls:analyze_details",
                      args=(DEMO_ACCOUNT_ID,))
        ad_groups = ["demo11", "demo22"]
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
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

