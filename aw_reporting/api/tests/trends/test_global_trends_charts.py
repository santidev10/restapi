import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from urllib.parse import urlencode

from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.charts.analytics_charts import Indicator
from aw_reporting.charts.base_chart import Breakdown
from aw_reporting.charts.base_chart import TrendId
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from userprofile.constants import StaticPermissions
from utils.datetime import as_datetime
from utils.lang import flatten
from utils.unittests.generic_test import generic_test
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse


class GlobalTrendsChartsTestCase(AwReportingAPITestCase):
    url = reverse(Name.GlobalTrends.CHARTS, [Namespace.AW_REPORTING])

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.CHF_TRENDS: True,
        })
        self.account, self.campaign, self.ad_group = self.create_data(next(int_iterator))

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
        account_2 = self.create_account(self.user, "2")
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
            indicator=Indicator.IMPRESSIONS,
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)

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
            indicator=Indicator.IMPRESSIONS,
            am="1"
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNone(trend)

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
            indicator=Indicator.IMPRESSIONS,
            am=am.id
        )
        url = "{}?{}".format(self.url, urlencode(filters))

        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)

    def test_filter_by_sales(self):
        today = datetime.now().date()
        _, campaign, ad_group = self.account, self.campaign, self.ad_group
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
            indicator=Indicator.IMPRESSIONS,
            sales=sales_1.id
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNone(trend)

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
            indicator=Indicator.IMPRESSIONS,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(set(i["value"] for i in trend[0]["trend"]),
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
            indicator=Indicator.VIEW_RATE,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one chart")
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend), 1)
        self.assertEqual(set(i["value"] for i in trend),
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
            indicator=Indicator.IMPRESSIONS,
            dimension="device",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertEqual(len(trend), 2)
        for line in response.data[0]["data"]:
            if line["label"] == "Computers":
                self.assertEqual(line["average"], test_impressions[0])
            else:
                self.assertEqual(line["average"], test_impressions[1])

    def test_success_dimension_channel(self):
        today = datetime.now().date()
        with open("saas/fixtures/tests/singledb_channel_list.json") as fd:
            data = json.load(fd)
            channel_ids = [i["id"] for i in data["items"]]
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
            indicator=Indicator.IMPRESSIONS,
            dimension="channel",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertEqual(len(trend), 10)

    def test_success_dimension_video(self):
        today = datetime.now().date()
        with open("saas/fixtures/tests/singledb_video_list.json") as fd:
            data = json.load(fd)
            ids = [i["id"] for i in data["items"]]
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
            indicator=Indicator.IMPRESSIONS,
            dimension="video",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
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
            indicator=Indicator.IMPRESSIONS,
            breakdown=Breakdown.HOURLY,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(len(trend[0]["trend"]), 48, "24 hours x 2 days")

    def test_planned_daily(self):
        account = self.account
        campaign_1 = self.campaign
        campaign_2 = Campaign.objects.create(id=2, account=account)
        any_date = date(2108, 4, 10)
        start_1, end_1 = any_date, any_date + timedelta(days=1)
        start_2, end_2 = any_date + timedelta(days=1), any_date + timedelta(
            days=2)
        start, end = min([start_1, start_2]), max([end_1, end_2])
        create_opportunity(campaign_1, id=1)
        create_opportunity(campaign_2, id=2)
        ordered_units_1, ordered_units_2 = 234, 345
        daily_plan_1 = ordered_units_1 / ((end_1 - start_1).days + 1)
        daily_plan_2 = ordered_units_2 / ((end_2 - start_2).days + 1)
        placement_1 = campaign_1.salesforce_placement
        placement_2 = campaign_2.salesforce_placement
        placement_1.ordered_units = ordered_units_1
        placement_2.ordered_units = ordered_units_2
        placement_1.goal_type_id = SalesForceGoalType.CPM
        placement_2.goal_type_id = SalesForceGoalType.CPM
        placement_1.start, placement_1.end = start_1, end_1
        placement_2.start, placement_2.end = start_2, end_2
        placement_1.save()
        placement_2.save()
        expected_planned_trend = [
            dict(label=start + timedelta(days=0), value=daily_plan_1),
            dict(label=start + timedelta(days=1),
                 value=daily_plan_1 + daily_plan_2),
            dict(label=start + timedelta(days=2), value=daily_plan_2),
        ]
        expected_planned_value = sum([
            r.get("value") for r in expected_planned_trend
        ])
        expected_planned_average = expected_planned_value \
                                   / len(expected_planned_trend)
        expected_planned_value = expected_planned_average

        filters = dict(
            start_date=start,
            end_date=end,
            indicator=Indicator.IMPRESSIONS
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend_info = get_trend(response.data, TrendId.PLANNED)[0]
        planned_trend = trend_info["trend"]
        self.assertEqual(planned_trend, expected_planned_trend)
        planned_value = trend_info["value"]
        self.assertEqual(planned_value, expected_planned_value)
        planned_average = trend_info["average"]
        self.assertEqual(planned_average, expected_planned_average)

    def test_planned_hourly(self):
        account = self.account
        campaign_1 = self.campaign
        campaign_2 = Campaign.objects.create(id=2, account=account)
        any_date = date(2108, 4, 10)
        start_1, end_1 = any_date, any_date + timedelta(days=1)
        start_2, end_2 = any_date + timedelta(days=1), any_date + timedelta(
            days=2)
        start, end = min([start_1, start_2]), max([end_1, end_2])
        create_opportunity(campaign_1, id=1)
        create_opportunity(campaign_2, id=2)
        ordered_units_1, ordered_units_2 = 234, 345
        daily_plan_1 = ordered_units_1 / ((end_1 - start_1).days + 1)
        daily_plan_2 = ordered_units_2 / ((end_2 - start_2).days + 1)
        placement_1 = campaign_1.salesforce_placement
        placement_2 = campaign_2.salesforce_placement
        placement_1.ordered_units = ordered_units_1
        placement_2.ordered_units = ordered_units_2
        placement_1.goal_type_id = SalesForceGoalType.CPM
        placement_2.goal_type_id = SalesForceGoalType.CPM
        placement_1.start, placement_1.end = start_1, end_1
        placement_2.start, placement_2.end = start_2, end_2
        placement_1.save()
        placement_2.save()
        expected_daily_planned_trend = [
            dict(label=start + timedelta(days=0), value=daily_plan_1),
            dict(label=start + timedelta(days=1),
                 value=daily_plan_1 + daily_plan_2),
            dict(label=start + timedelta(days=2), value=daily_plan_2),
        ]
        expected_planned_value = sum([
            r.get("value") for r in expected_daily_planned_trend
        ])
        expected_planned_average = expected_planned_value \
                                   / len(expected_daily_planned_trend)
        expected_planned_value = expected_planned_average

        def expand(item):
            label, value = item["label"], item["value"]
            return (dict(label=as_datetime(label) + timedelta(hours=hour),
                         value=value / 24)
                    for hour in range(24))

        expected_planned_trend = flatten(expand(i)
                                         for i in expected_daily_planned_trend)
        filters = dict(
            start_date=start,
            end_date=end,
            indicator=Indicator.IMPRESSIONS,
            breakdown=Breakdown.HOURLY,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend_info = get_trend(response.data, TrendId.PLANNED)[0]
        planned_trend = trend_info["trend"]
        self.assertEqual(planned_trend, expected_planned_trend)
        planned_value = trend_info["value"]
        self.assertEqual(planned_value, expected_planned_value)
        planned_average = trend_info["average"]
        self.assertEqual(planned_average, expected_planned_average)

    def test_planned_cpv(self):
        account = self.account
        campaign_1 = self.campaign
        campaign_2 = Campaign.objects.create(id=2, account=account)
        any_date = date(2108, 4, 10)
        start_1, end_1 = any_date, any_date
        start_2, end_2 = any_date, any_date
        start, end = any_date, any_date
        create_opportunity(campaign_1, id=1)
        create_opportunity(campaign_2, id=2)
        ordered_units_1, ordered_units_2 = 234, 345
        total_cost_1, total_cost_2 = 100, 100
        placement_1 = campaign_1.salesforce_placement
        placement_2 = campaign_2.salesforce_placement
        placement_1.ordered_units = ordered_units_1
        placement_2.ordered_units = ordered_units_2
        placement_1.total_cost = total_cost_1
        placement_2.total_cost = total_cost_2
        placement_1.goal_type_id = SalesForceGoalType.CPV
        placement_2.goal_type_id = SalesForceGoalType.CPV
        placement_1.start, placement_1.end = start_1, end_1
        placement_2.start, placement_2.end = start_2, end_2
        placement_1.save()
        placement_2.save()
        expected_planned_trend = [
            dict(label=any_date, value=sum([total_cost_1, total_cost_2]) / sum([ordered_units_1, ordered_units_2])),
        ]
        expected_planned_value = sum([
            r.get("value") for r in expected_planned_trend
        ])
        expected_planned_average = expected_planned_value \
                                   / len(expected_planned_trend)

        filters = dict(
            start_date=start,
            end_date=end,
            indicator=Indicator.CPV
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.PLANNED)
        self.assertIsNotNone(trend)
        planned_trend = trend[0]["trend"]
        self.assertEqual(planned_trend, expected_planned_trend)
        planned_value = trend[0]["value"]
        self.assertEqual(planned_value, expected_planned_value)
        planned_average = trend[0]["average"]
        self.assertEqual(planned_average, expected_planned_average)

    def test_planned_cpm(self):
        account = self.account
        campaign_1 = self.campaign
        campaign_2 = Campaign.objects.create(id=2, account=account)
        any_date = date(2108, 4, 10)
        start_1, end_1 = any_date, any_date
        start_2, end_2 = any_date, any_date
        start, end = any_date, any_date
        create_opportunity(campaign_1, id=1)
        create_opportunity(campaign_2, id=2)
        ordered_units_1, ordered_units_2 = 234, 345
        total_cost_1, total_cost_2 = 47, 47
        placement_1 = campaign_1.salesforce_placement
        placement_2 = campaign_2.salesforce_placement
        placement_1.ordered_units = ordered_units_1
        placement_2.ordered_units = ordered_units_2
        placement_1.total_cost = total_cost_1
        placement_2.total_cost = total_cost_2
        placement_1.goal_type_id = SalesForceGoalType.CPM
        placement_2.goal_type_id = SalesForceGoalType.CPM
        placement_1.start, placement_1.end = start_1, end_1
        placement_2.start, placement_2.end = start_2, end_2
        placement_1.save()
        placement_2.save()
        expected_planned_trend = [
            dict(label=any_date,
                 value=sum([total_cost_1, total_cost_2]) / sum([ordered_units_1 / 1000., ordered_units_2 / 1000.])),
        ]
        expected_planned_value = sum([
            r.get("value") for r in expected_planned_trend
        ])
        expected_planned_average = expected_planned_value \
                                   / len(expected_planned_trend)
        filters = dict(
            start_date=start,
            end_date=end,
            indicator=Indicator.CPM
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.PLANNED)
        self.assertIsNotNone(trend)
        planned_trend = trend[0]["trend"]
        self.assertEqual(planned_trend, expected_planned_trend)
        planned_value = trend[0]["value"]
        self.assertEqual(planned_value, expected_planned_value)
        planned_average = trend[0]["average"]
        self.assertEqual(planned_average, expected_planned_average)

    def test_planned_cpv_hourly(self):
        account = self.account
        campaign_1 = self.campaign
        campaign_2 = Campaign.objects.create(id=2, account=account)
        any_date = date(2108, 4, 10)
        start_1, end_1 = any_date, any_date
        start_2, end_2 = any_date, any_date
        start, end = any_date, any_date
        create_opportunity(campaign_1, id=1)
        create_opportunity(campaign_2, id=2)
        ordered_units_1, ordered_units_2 = 234, 345
        total_cost_1, total_cost_2 = 100, 100
        placement_1 = campaign_1.salesforce_placement
        placement_2 = campaign_2.salesforce_placement
        placement_1.ordered_units = ordered_units_1
        placement_2.ordered_units = ordered_units_2
        placement_1.total_cost = total_cost_1
        placement_2.total_cost = total_cost_2
        placement_1.goal_type_id = SalesForceGoalType.CPV
        placement_2.goal_type_id = SalesForceGoalType.CPV
        placement_1.start, placement_1.end = start_1, end_1
        placement_2.start, placement_2.end = start_2, end_2
        placement_1.save()
        placement_2.save()
        expected_planned_trend = [
            dict(label=as_datetime(any_date) + timedelta(hours=i),
                 value=sum([total_cost_1, total_cost_2]) / sum([ordered_units_1, ordered_units_2]))
            for i in range(24)
        ]

        filters = dict(
            start_date=start,
            end_date=end,
            indicator="average_cpv",
            breakdown=Breakdown.HOURLY
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.PLANNED)
        self.assertIsNotNone(trend)
        planned_trend = trend[0]["trend"]
        self.assertEqual(planned_trend, expected_planned_trend)

    @generic_test((
        ("Show AW rates", (True,), {}),
        ("Hide AW rates", (False,), {}),
    ))
    def test_aw_rate_settings_does_not_affect_rates(self, aw_rates):
        """
        Bug: Trends > "Show real (AdWords) costs on the dashboard" affects data on Media Buying -> Trends
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2818
        """
        account = self.account
        manager = account.managers.first()
        views, cost = 12, 23
        any_date = date(2018, 1, 1)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, date=any_date, video_views=views, cost=cost,
                                        average_position=1)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))

        expected_cpv = cost / views
        self.assertGreater(expected_cpv, 0)

        self.user.perms[StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST] = aw_rates
        self.user.save()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            trend = get_trend(response.data, TrendId.HISTORICAL)
            historical_cpv = trend[0]["trend"]
            self.assertEqual(len(historical_cpv), 1)
            item = historical_cpv[0]
            self.assertAlmostEqual(item["value"], expected_cpv)

    @generic_test([
        ("Global account visibility is ON", (True, True), dict()),
        ("Global account visibility is OFF", (False, False), dict()),
    ])
    def test_global_account_visibility(self, global_account_visibility, is_none):
        self.user = self.create_test_user(perms={
            StaticPermissions.CHF_TRENDS: True,
            StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY: global_account_visibility
        })
        account = self.account
        manager = account.managers.first()
        any_date = date(2018, 1, 1)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, date=any_date, video_views=1, cost=1,
                                        average_position=1)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings), \
             override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            trend = get_trend(response.data, TrendId.HISTORICAL)
            if is_none:
                self.assertIsNone(trend)
            else:
                self.assertIsNotNone(trend)

    def test_filters_without_dates(self):
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
            indicator=Indicator.IMPRESSIONS,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        trend = get_trend(response.data, TrendId.HISTORICAL)
        self.assertIsNotNone(trend)
        self.assertEqual(set(i["value"] for i in trend[0]["trend"]),
                         {test_impressions})

    def test_request_without_date_filter(self):
        start = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            start=start,
        )
        self.campaign.salesforce_placement = placement
        self.campaign.save()

        filters = dict(
            indicator=Indicator.CPV,
            breakdown=Breakdown.HOURLY,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        manager = self.campaign.account.managers.first()
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)


def get_trend(data, uid):
    trends = dict(((t["id"], t["data"])
                   for t in data))
    return trends.get(uid) or None


def create_opportunity(campaign, **kwargs):
    uid = kwargs.pop("id")
    opportunity = Opportunity.objects.create(id=uid, **kwargs)
    placement = OpPlacement.objects.create(id=uid, opportunity=opportunity)
    campaign.salesforce_placement = placement
    campaign.save()
    return opportunity
