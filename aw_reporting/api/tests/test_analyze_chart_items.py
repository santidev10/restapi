import json
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.demo.models import *
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
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
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name="Babruysk"))
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
        url = reverse("aw_reporting_urls:analyze_chart_items",
                      args=(self.account.id, 'ad'))

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
        self.assertEqual(len(data['items']), 1)
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
                "video_clicks",
            }
        )

    def test_success_get_filter_items(self):
        url = reverse("aw_reporting_urls:analyze_chart_items",
                      args=(self.account.id, 'ad'))
        today = datetime.now().date()
        start_date = str(today - timedelta(days=2))
        end_date = str(today - timedelta(days=1))
        response = self.client.post(
            url,
            json.dumps(dict(start_date=start_date,
                            end_date=end_date,
                            campaigns=[1])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)

        response = self.client.post(
            url,
            json.dumps(dict(start_date=start_date,
                            end_date=end_date,
                            campaigns=[2])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 0)

    def test_demo_all_dimensions(self):
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ('device', 'gender', 'age', 'topic',
                              'interest', 'creative', 'channel', 'video',
                              'keyword', 'location', 'ad'):

                url = reverse("aw_reporting_urls:analyze_chart_items",
                              args=(self.account.id, dimension))
                response = self.client.post(url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertGreater(len(response.data), 1)

