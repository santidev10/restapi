import json
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.charts import ALL_DIMENSIONS
from aw_reporting.demo.models import *
from utils.utils_tests import SingleDatabaseApiConnectorPatcher, generic_test
from .base import AwReportingAPITestCase


class AccountNamesAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.account = self.create_account(self.user)
        self.create_stats(self.account)

    @staticmethod
    def create_stats(account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1, defaults=dict(name="boo", type="A"))
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name="bobruisk"))
        ad = Ad.objects.create(id=1, ad_group=ad_group1)
        AdStatistic.objects.create(ad=ad, average_position=1, **base_stats)

        for ad_group in (ad_group1, ad_group2):
            stats = dict(ad_group=ad_group, **base_stats)
            AdGroupStatistic.objects.create(average_position=1, **stats)
            GenderStatistic.objects.create(**stats)
            AgeRangeStatistic.objects.create(**stats)
            TopicStatistic.objects.create(topic=topic, **stats)
            AudienceStatistic.objects.create(audience=audience, **stats)
            VideoCreativeStatistic.objects.create(creative=creative, **stats)
            YTChannelStatistic.objects.create(yt_id="123", **stats)
            YTVideoStatistic.objects.create(yt_id="123", **stats)
            KeywordStatistic.objects.create(keyword="123", **stats)
            CityStatistic.objects.create(city=city, **stats)

    def test_success_get_filter_dates(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(self.account.id,))

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
        self.assertEqual(data[1]['title'], "#1")
        self.assertEqual(data[2]['title'], "#2")

    def test_success_get_filter_items(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(self.account.id,))
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url,
                json.dumps(dict(campaigns=["1"])),
                content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], "#1")
        self.assertEqual(len(data[0]['data'][0]['trend']), 1)

    @generic_test([
        (dimension, (dimension,), dict())
        for dimension in ALL_DIMENSIONS
    ], [0])
    def test_all_dimensions(self, dimension):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(self.account.id,))
        filters = {
            'indicator': 'video_view_rate',
            'dimension': dimension
        }
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(filters),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(len(response.data[0]['data']), 1)
