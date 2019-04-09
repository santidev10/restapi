import json
from datetime import datetime, timedelta, date
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.analytics_charts import TrendId, Indicator, Breakdown
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic, YTChannelStatistic, YTVideoStatistic
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from utils.utittests.generic_test import generic_test
from utils.utittests.patch_now import patch_now


class TrackChartAPITestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Track.CHART)

    def setUp(self):
        user = self.create_test_user()
        account = self.create_account(user)
        self.campaign = Campaign.objects.create(
            id="1", name="", account=account)
        self.ad_group = AdGroup.objects.create(
            id="1", name="", campaign=self.campaign
        )

    def test_success_get(self):
        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            AdGroupStatistic.objects.create(
                ad_group=self.ad_group,
                average_position=1,
                date=today - timedelta(days=i),
                impressions=test_impressions,
            )

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(set(i['value'] for i in trend[0]["trend"]),
                         {test_impressions})

    def test_success_get_view_rate_calculation(self):
        cpm_ad_group = AdGroup.objects.create(
            id="2", name="", campaign=self.campaign
        )
        today = datetime.now().date()
        test_impressions = 10
        for video_views, ad_group in enumerate((cpm_ad_group, self.ad_group)):
            AdGroupStatistic.objects.create(
                ad_group=ad_group,
                average_position=1,
                date=today,
                impressions=test_impressions,
                video_views=video_views,
            )
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="video_view_rate",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend), 1)
        self.assertEqual(set(i['value'] for i in trend),
                         {10})  # 10% video view rate

    def test_success_dimension_device(self):
        today = datetime.now().date()
        test_days = 10
        test_impressions = (100, 50)
        for i in range(test_days):
            for device_id in (0, 1):
                AdGroupStatistic.objects.create(
                    ad_group=self.ad_group,
                    average_position=1,
                    device_id=device_id,
                    date=today - timedelta(days=i),
                    impressions=test_impressions[device_id],
                )

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="device",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend), 2)
        for line in trend:
            if line['label'] == "Computers":
                self.assertEqual(line['average'], test_impressions[0])
            else:
                self.assertEqual(line['average'], test_impressions[1])

    def test_success_dimension_channel(self):
        today = datetime.now().date()
        with open("saas/fixtures/tests/singledb_channel_list.json") as fd:
            data = json.load(fd)
            channel_ids = [i['id'] for i in data['items']]
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            for n, channel_id in enumerate(channel_ids):
                YTChannelStatistic.objects.create(
                    ad_group=self.ad_group,
                    yt_id=channel_id,
                    date=today - timedelta(days=i),
                    impressions=test_impressions * n,
                )

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="channel",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend), 10)

    def test_success_dimension_video(self):
        today = datetime.now().date()
        with open("saas/fixtures/tests/singledb_video_list.json") as fd:
            data = json.load(fd)
            ids = [i['id'] for i in data['items']]
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            for n, uid in enumerate(ids):
                YTVideoStatistic.objects.create(
                    ad_group=self.ad_group,
                    yt_id=uid,
                    date=today - timedelta(days=i),
                    impressions=test_impressions * n,
                )

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="video",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend), 10)

    def test_success_hourly(self):
        today = datetime.now().date()
        test_days = 10
        for i in range(test_days):
            for hour in range(24):
                CampaignHourlyStatistic.objects.create(
                    campaign=self.campaign,
                    date=today - timedelta(days=i),
                    hour=hour,
                    impressions=hour,
                )

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend[0]["trend"]), 48, "24 hours x 2 days")

    @generic_test((
            ("Show AW rates", (True,), {}),
            ("Hide AW rates", (False,), {}),
    ))
    def test_aw_rate_settings_does_not_affect_rates(self, aw_rates):
        """
        Bug: Trends > "Show real (AdWords) costs on the dashboard" affects data on Media Buying -> Trends
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2818
        """
        views, cost = 12, 23
        any_date = date(2018, 1, 1)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, date=any_date, video_views=views, cost=cost,
                                        average_position=1)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))

        expected_cpv = cost / views
        self.assertGreater(expected_cpv, 0)
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rates
        }
        with patch_now(any_date), \
             self.patch_user_settings(**user_settings):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            trend = get_trend(response.data, TrendId.HISTORICAL)
            historical_cpv = trend[0]["trend"]
            self.assertEqual(len(historical_cpv), 1)
            item = historical_cpv[0]
            self.assertAlmostEqual(item["value"], expected_cpv)


def get_trend(data, uid):
    trends = dict(((t["id"], t["data"])
                   for t in data))
    return trends.get(uid) or None
