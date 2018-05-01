from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from django.utils.http import urlencode
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic, Account, User, Opportunity, OpPlacement, \
    SalesForceGoalType, GeoTarget, CampaignLocationTargeting
from aw_reporting.settings import InstanceSettingsKey
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.query import Operator
from utils.utils_tests import patch_instance_settings


class GlobalTrendsDataTestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.GlobalTrends.DATA)

    # def setUp(self):
    #     User.objects.all().delete()
    #     Account.objects.all().delete()

    def _create_test_data(self, uid=1):
        user = self.create_test_user()
        account = self.create_account(user, "{}-".format(uid))
        campaign = Campaign.objects.create(
            id=uid, name="", account=account)
        ad_group = AdGroup.objects.create(
            id=uid, name="", campaign=campaign
        )
        return user, account, campaign, ad_group

    def test_authorization_required(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_success_daily(self):
        _, account, _, ad_group = self._create_test_data()
        manager = account.managers.first()
        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            AdGroupStatistic.objects.create(
                ad_group=ad_group,
                average_position=1,
                date=today - timedelta(days=i),
                impressions=test_impressions,
            )

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(
            set(account_data.keys()),
            {
                'id',
                'label',
                'average_1d',
                'average_5d',
                'trend',
            }
        )
        self.assertEqual(len(account_data['trend']), 2)

    def test_success_filter_account(self):
        _, account, _, ad_group_1 = self._create_test_data()
        manager = account.managers.first()
        account = Account.objects.create(id=2, name="Name")
        account.managers.add(manager)
        campaign = Campaign.objects.create(id=2, name="", account=account)
        ad_group = AdGroup.objects.create(id=2, name="", campaign=campaign)

        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for ag in (ad_group_1, ad_group):
            for i in range(test_days):
                AdGroupStatistic.objects.create(
                    ad_group=ag,
                    average_position=1,
                    date=today - timedelta(days=i),
                    impressions=test_impressions,
                )

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            account=account.id,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    def test_success_hourly(self):
        _, account, campaign, _ = self._create_test_data()
        manager = account.managers.first()
        today = datetime.now().date()
        test_days = 10
        for i in range(test_days):
            for hour in range(24):
                CampaignHourlyStatistic.objects.create(
                    campaign=campaign,
                    date=today - timedelta(days=i),
                    hour=hour,
                    impressions=hour,
                )

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account = response.data[0]

        self.assertEqual(
            set(account.keys()),
            {
                'id',
                'label',
                'average_1d',
                'average_5d',
                'trend',
            }
        )
        self.assertEqual(len(account['trend']), 2 * 24)

    def _create_ad_group_statistic(self, uid):
        _, account, campaign, ad_group = self._create_test_data(uid)
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        # campaign = Campaign.objects.create(id=uid, account=account)
        # ad_group = AdGroup.objects.create(id=uid, campaign=campaign)
        AdGroupStatistic.objects.create(date=yesterday, ad_group=ad_group,
                                        video_views=1, average_position=1)
        return account, campaign

    def test_filter_manage_account(self):
        # self.create_test_user()
        account, _ = self._create_ad_group_statistic("rel")
        self._create_ad_group_statistic("irr")
        manager = account.managers.first()

        self._create_ad_group_statistic(1)
        self._create_ad_group_statistic(2)

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    def _create_opportunity(self, campaign, **kwargs):
        uid = kwargs.pop("uid")
        opportunity = Opportunity.objects.create(id=uid, **kwargs)
        placement = OpPlacement.objects.create(id=uid, opportunity=opportunity)
        campaign.salesforce_placement = placement
        campaign.save()

    def test_filter_am(self):
        am_1 = User.objects.create(id=1)
        am_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 account_manager=am_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 account_manager=am_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(am=am_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_ad_ops(self):
        ad_ops_1 = User.objects.create(id=1)
        ad_ops_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 ad_ops_manager=ad_ops_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 ad_ops_manager=ad_ops_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(ad_ops=ad_ops_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_sales(self):
        sales_1 = User.objects.create(id=1)
        sales_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 sales_manager=sales_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 sales_manager=sales_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(sales=sales_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_brands(self):
        brand_1 = "Test Brand 1"
        brand_2 = "Test Brand 2"
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 brand=brand_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 brand=brand_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(brands=brand_1)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_goal_types(self):
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1)
        self._create_opportunity(uid=2, campaign=campaign_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()
        campaign_1.salesforce_placement.goal_type_id = SalesForceGoalType.CPV
        campaign_1.salesforce_placement.save()
        campaign_2.salesforce_placement.goal_type_id = SalesForceGoalType.CPM
        campaign_2.salesforce_placement.save()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(goal_type=SalesForceGoalType.CPV)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_categories(self):
        category_1 = "Test Category 1"
        category_2 = "Test Category 2"
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 category_id=category_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 category_id=category_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(category=category_1)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_geo(self):
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1)
        self._create_opportunity(uid=2, campaign=campaign_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()
        geo_target_1 = GeoTarget.objects.create(id="1")
        geo_target_2 = GeoTarget.objects.create(id="2")
        CampaignLocationTargeting.objects.create(
            campaign=campaign_1, location=geo_target_1)
        CampaignLocationTargeting.objects.create(
            campaign=campaign_2, location=geo_target_2)

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(geo_locations=geo_target_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_geo_or(self):
        account_1, campaign_1 = self._create_ad_group_statistic("rel 1")
        account_2, campaign_2 = self._create_ad_group_statistic("rel 2")
        account_3, campaign_3 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1)
        self._create_opportunity(uid=2, campaign=campaign_2)
        self._create_opportunity(uid=3, campaign=campaign_3)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()
        manager_3 = account_3.managers.first()
        geo_target_1 = GeoTarget.objects.create(id="1")
        geo_target_2 = GeoTarget.objects.create(id="2")
        geo_target_3 = GeoTarget.objects.create(id="3")
        CampaignLocationTargeting.objects.create(
            campaign=campaign_1, location=geo_target_1)
        CampaignLocationTargeting.objects.create(
            campaign=campaign_2, location=geo_target_2)
        CampaignLocationTargeting.objects.create(
            campaign=campaign_3, location=geo_target_3)

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id,
                                                         manager_3.id]
        }
        filters = dict(
            geo_locations=",".join([geo_target_1.id, geo_target_2.id]),
            geo_locations_condition=Operator.OR)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        account_ids = [acc["id"] for acc in response.data]
        self.assertEqual(account_ids, sorted([account_1.id, account_2.id]))

    def test_filter_geo_and(self):
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        account_2, campaign_2 = self._create_ad_group_statistic("irr")
        self._create_opportunity(uid=1, campaign=campaign_1)
        self._create_opportunity(uid=2, campaign=campaign_2)
        manager_1 = account_1.managers.first()
        manager_2 = account_2.managers.first()
        geo_target_1 = GeoTarget.objects.create(id="1")
        geo_target_2 = GeoTarget.objects.create(id="2")
        CampaignLocationTargeting.objects.create(
            campaign=campaign_1, location=geo_target_1)
        CampaignLocationTargeting.objects.create(
            campaign=campaign_1, location=geo_target_2)
        CampaignLocationTargeting.objects.create(
            campaign=campaign_2, location=geo_target_2)

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager_1.id,
                                                         manager_2.id]
        }
        filters = dict(
            geo_locations=",".join([geo_target_1.id, geo_target_2.id]),
            geo_locations_condition=Operator.AND)
        url = "{}?{}".format(self.url, urlencode(filters))
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)
