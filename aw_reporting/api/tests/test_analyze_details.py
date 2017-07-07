import json
from unittest.mock import patch
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from .base import AwReportingAPITestCase


class AccountDetailsAPITestCase(AwReportingAPITestCase):
    overview_keys = {
        'age', 'gender', 'device', 'location',
        'clicks', 'cost', 'impressions', 'video_views',
        'ctr', 'ctr_v', 'average_cpm', 'average_cpv',
        "all_conversions", "conversions", "view_through",
        'video_view_rate',
        'video100rate', 'video25rate', 'video50rate',
        'video75rate', 'video_views_this_week',
        'video_view_rate_top', 'impressions_this_week',
        'video_views_last_week', 'cost_this_week',
        'video_view_rate_bottom', 'clicks_this_week',
        'ctr_v_top', 'cost_last_week', 'average_cpv_top',
        'ctr_v_bottom', 'ctr_bottom', 'clicks_last_week',
        'average_cpv_bottom', 'ctr_top', 'impressions_last_week',
    }

    detail_keys = {
        'creative',
        'age', 'gender', 'device',
        "all_conversions", "conversions", "view_through", 'average_position',
        'video100rate', 'video25rate', 'video50rate', 'video75rate',
        'delivery_trend',
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        account = self.create_account(self.user)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(
            id=1, name="", account=account, **stats
        )
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        date = datetime.now().date() - timedelta(days=1)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=date, average_position=1, **stats)
        target, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name=""))
        CityStatistic.objects.create(ad_group=ad_group, date=date, city=target, **stats)

        url = reverse("aw_reporting_urls:analyze_details",
                      args=(account.id,))

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url,
                json.dumps(dict(start_date=str(date - timedelta(days=1)),
                                end_date=str(date))),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertEqual(
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"},
        )
        self.assertEqual(
            set(data["details"].keys()),
            self.detail_keys,
        )
        self.assertEqual(data['details']['video25rate'], 100)
        self.assertEqual(data['details']['video50rate'], 75)
        self.assertEqual(data['details']['video75rate'], 50)
        self.assertEqual(data['details']['video100rate'], 25)
        self.assertEqual(
            set(data["overview"].keys()),
            self.overview_keys,
        )

    def test_success_get_two_connections(self):
        account = self.create_account(self.user)

        # create second account + account connection for that manager
        manager = account.managers.first()
        connection = AWConnection.objects.create(
            email="another_user@email.com",
            refresh_token="",
        )
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=self.user,
        )
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )

        url = reverse("aw_reporting_urls:analyze_details",
                      args=(account.id,))

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

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
            self.account_list_header_fields | {"details", "overview"},
        )
        self.assertEqual(
            set(data["details"].keys()),
            self.detail_keys,
        )
        self.assertEqual(
            set(data["overview"].keys()),
            self.overview_keys,
        )
        self.assertEqual(data["details"]['delivery_trend'][0]['label'], "Impressions")
        self.assertEqual(data["details"]['delivery_trend'][1]['label'], "Views")
        self.assertEqual(data['overview']['impressions'], IMPRESSIONS / 10)

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
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"},
        )
        self.assertEqual(
            set(data["details"].keys()),
            self.detail_keys,
        )
        self.assertEqual(
            set(data["overview"].keys()),
            self.overview_keys,
        )
        self.assertEqual(
            data['overview']['impressions'],
            IMPRESSIONS / TOTAL_DEMO_AD_GROUPS_COUNT * len(ad_groups),
        )

