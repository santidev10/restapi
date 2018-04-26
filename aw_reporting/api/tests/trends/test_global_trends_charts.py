import json
from datetime import datetime, timedelta
from unittest.mock import patch
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic, YTChannelStatistic, YTVideoStatistic, User, \
    Opportunity, OpPlacement
from aw_reporting.settings import InstanceSettingsKey
from saas.urls.namespaces import Namespace
from utils.utils_tests import SingleDatabaseApiConnectorPatcher, \
    patch_instance_settings


class GlobalTrendsChartsTestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.GlobalTrends.CHARTS)

    def setUp(self):
        self.user = self.create_test_user()
        self.account, self.campaign, self.ad_group = self.create_data("1")

    def create_data(self, uid):
        account = self.create_account(self.user, uid)
        campaign = Campaign.objects.create(
            id=uid, name="", account=account)
        ad_group = AdGroup.objects.create(
            id=uid, name="", campaign=campaign
        )
        return account, campaign, ad_group

    def test_authorization_required(self):
        self.user.delete()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_filters_by_global_account(self):
        today = datetime.now().date()
        test_impressions = 100
        account_2 = self.create_account(self.user, "2-")
        campaign_2 = Campaign.objects.create(
            id="2", name="", account=account_2)
        ad_group_2 = AdGroup.objects.create(
            id="2", name="", campaign=campaign_2
        )
        for ad_group in (self.ad_group, ad_group_2):
            AdGroupStatistic.objects.create(
                ad_group=ad_group,
                average_position=1,
                date=today - timedelta(days=1),
                impressions=test_impressions,
            )

        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="impressions",
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]["data"]), 1)

    def test_filter_by_am_negative(self):
        today = datetime.now().date()
        AdGroupStatistic.objects.create(
            ad_group=self.ad_group,
            average_position=1,
            date=today - timedelta(days=1),
            impressions=100,
        )
        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="impressions",
            am="1"
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["data"]), 0)

    def test_filter_by_am_positive(self):
        am = User.objects.create(id="12")
        opportunity = Opportunity.objects.create(account_manager=am)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        self.campaign.salesforce_placement = placement
        self.campaign.save()
        today = datetime.now().date()
        AdGroupStatistic.objects.create(
            ad_group=self.ad_group,
            average_position=1,
            date=today - timedelta(days=1),
            impressions=100,
        )
        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="impressions",
            am=am.id
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["data"]), 1)

    def test_filter_by_sales(self):
        today = datetime.now().date()
        account, campaign, ad_group = self.account, self.campaign, self.ad_group
        AdGroupStatistic.objects.create(
            ad_group=ad_group,
            average_position=1,
            date=today - timedelta(days=1),
            impressions=100,
        )
        sales_1 = User.objects.create(id="sales 1")
        create_opportunity(id="1", campaign=campaign)
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            sales=sales_1.id
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["data"]), 0)

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
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
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
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=1),
            end_date=today,
            indicator="video_view_rate",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        self.assertEqual(len(response.data[0]['data']), 1, "one line")
        trend = response.data[0]['data'][0]['trend']
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
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
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

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="channel",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch_instance_settings(**instance_settings):
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
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch_instance_settings(**instance_settings):
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

        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = response.data[0]['data'][0]['trend']
        self.assertEqual(len(trend), 48, "24 hours x 2 days")


def create_opportunity(campaign, **kwargs):
    uid = kwargs.pop("id")
    opportunity = Opportunity.objects.create(id=uid, **kwargs)
    placement = OpPlacement.objects.create(id=uid, opportunity=opportunity)
    campaign.salesforce_placement = placement
    campaign.save()
    return opportunity
