from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from .base import AwReportingAPITestCase
from datetime import datetime, timedelta
from urllib.parse import urlencode
from aw_reporting.models import Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic, YTChannelStatistic, YTVideoStatistic
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
import json


class TrackChartAPITestCase(AwReportingAPITestCase):

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
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 2)
        self.assertEqual(set(i['value'] for i in trend),
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
        url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="video_view_rate",
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 1, "one line")
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 1)
        self.assertEqual(set(i['value'] for i in trend), {10})  # 10% video view rate

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

        base_url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="device",
        )
        url = "{}?{}".format(base_url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]['data']), 2)
        for line in response.data[0]['data']:
            if line['label'] == "Computers":
                self.assertEqual(line['average'], test_impressions[0])
            else:
                self.assertEqual(line['average'], test_impressions[1])

    def test_success_dimension_channel(self):
        today = datetime.now().date()
        with open("saas/fixtures/singledb_channel_list.json") as fd:
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

        base_url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="channel",
        )
        url = "{}?{}".format(base_url, urlencode(filters))
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]['data']), 10)

    def test_success_dimension_video(self):
        today = datetime.now().date()
        with open("saas/fixtures/singledb_video_list.json") as fd:
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

        base_url = reverse("aw_reporting_urls:track_chart")
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="video",
        )
        url = "{}?{}".format(base_url, urlencode(filters))
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]['data']), 10)

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
