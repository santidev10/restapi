from datetime import datetime
from datetime import timedelta

from django.db.models import Sum
from django.test import override_settings
from django.utils.http import urlencode
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.analytics_charts import Breakdown
from aw_reporting.analytics_charts import Indicator
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import Category
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse


class GlobalTrendsDataTestCase(AwReportingAPITestCase):
    url = reverse(Name.GlobalTrends.DATA, [Namespace.AW_REPORTING])

    def _create_test_data(self, uid=1, manager=None):
        user = self.create_test_user()
        account = self.create_account(user, "{}-".format(uid), manager)
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
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
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
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
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
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
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

    def _create_ad_group_statistic(self, uid, manager=None):
        _, account, campaign, ad_group = self._create_test_data(uid, manager)
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        AdGroupStatistic.objects.create(date=yesterday, ad_group=ad_group,
                                        video_views=1, average_position=1,
                                        cost=1)
        return account, campaign

    def test_filter_manage_account(self):
        account, _ = self._create_ad_group_statistic("rel")
        self._create_ad_group_statistic("irr")
        manager = account.managers.first()

        self._create_ad_group_statistic(1)
        self._create_ad_group_statistic(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    def _create_opportunity(self, campaign, **kwargs):
        uid = kwargs.pop("uid", None) or next(int_iterator)
        opportunity = Opportunity.objects.create(id=uid, **kwargs)
        placement = OpPlacement.objects.create(id=uid, opportunity=opportunity)
        campaign.salesforce_placement = placement
        campaign.save()

    def test_filter_am(self):
        am_1 = User.objects.create(id=1)
        am_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 account_manager=am_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 account_manager=am_2)

        filters = dict(am=am_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_ad_ops(self):
        ad_ops_1 = User.objects.create(id=1)
        ad_ops_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 ad_ops_manager=ad_ops_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 ad_ops_manager=ad_ops_2)

        filters = dict(ad_ops=ad_ops_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_sales(self):
        sales_1 = User.objects.create(id=1)
        sales_2 = User.objects.create(id=2)
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 sales_manager=sales_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 sales_manager=sales_2)
        filters = dict(sales=sales_1.id)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_brands(self):
        brand_1 = "Test Brand 1"
        brand_2 = "Test Brand 2"
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 brand=brand_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 brand=brand_2)

        filters = dict(brands=brand_1)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_goal_types(self):
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1)
        self._create_opportunity(uid=2, campaign=campaign_2)
        campaign_1.salesforce_placement.goal_type_id = SalesForceGoalType.CPV
        campaign_1.salesforce_placement.save()
        campaign_2.salesforce_placement.goal_type_id = SalesForceGoalType.CPM
        campaign_2.salesforce_placement.save()

        filters = dict(goal_type=SalesForceGoalType.CPV)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_categories(self):
        category_1 = Category.objects.create(id="Test Category 1")
        category_2 = Category.objects.create(id="Test Category 2")
        account_1, campaign_1 = self._create_ad_group_statistic("rel")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("irr",
                                                                manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1,
                                 category=category_1)
        self._create_opportunity(uid=2, campaign=campaign_2,
                                 category=category_2)
        filters = dict(category=category_1)
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], account_1.id)

    def test_filter_geo(self):
        test_region_1 = "Region 1"
        test_region_2 = "Region 2"
        test_region_3 = "Region 3"
        account_1, campaign_1 = self._create_ad_group_statistic("rel_1")
        manager = account_1.managers.first()
        account_2, campaign_2 = self._create_ad_group_statistic("rel_2", manager=manager)
        account_3, campaign_3 = self._create_ad_group_statistic("irr", manager=manager)
        self._create_opportunity(uid=1, campaign=campaign_1, territory=test_region_1)
        self._create_opportunity(uid=2, campaign=campaign_2, territory=test_region_2)
        self._create_opportunity(uid=3, campaign=campaign_3, territory=test_region_3)

        filters = dict(region=",".join([test_region_1, test_region_2]))
        url = "{}?{}".format(self.url, urlencode(filters))
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        response_ids = set([acc["id"] for acc in response.data])
        self.assertEqual(response_ids, {account_1.id, account_2.id})

    @generic_test((
            ("Show AW rates", (True,), {}),
            ("Hide AW rates", (False,), {}),
    ))
    def test_aw_rate_settings_does_not_affect_rates(self, aw_rates):
        """
        Bug: CHF Trends > "Show real (AdWords) costs on the dashboard"
            affects data on CHF Trends
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2779
        """
        account, campaign = self._create_ad_group_statistic("rel_1")
        manager = account.managers.first()
        self._create_opportunity(campaign)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rates
        }
        stats = AdGroupStatistic.objects.all() \
            .aggregate(views=Sum("video_views"), cost=Sum("cost"))
        expected_cpv = stats["views"] / stats["cost"]
        self.assertGreater(expected_cpv, 0)
        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id), \
             self.patch_user_settings(**user_settings):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 1)
            self.assertEqual(len(response.data[0]["trend"]), 1)
            item = response.data[0]
            self.assertIsNotNone(item["average_1d"])
            self.assertAlmostEqual(item["average_1d"], expected_cpv)
            self.assertAlmostEqual(item["trend"][0]["value"], expected_cpv)

    @generic_test([
        ("Global account visibility is ON", (True, 0), dict()),
        ("Global account visibility is OFF", (False, 1), dict()),
    ])
    def test_global_account_visibility(self, global_account_visibility, items_count):
        account, campaign = self._create_ad_group_statistic("rel_1")
        manager = account.managers.first()
        self._create_opportunity(campaign)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: global_account_visibility,
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings), \
             override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), items_count)

    @generic_test([
        ("Visible all accounts is ON", (True, 1), dict()),
        ("Visible all accounts is OFF", (False, 0), dict()),
    ])
    def test_visible_all_accounts(self, visible_all_accounts, expected_count):
        account, campaign = self._create_ad_group_statistic("rel_1")
        manager = account.managers.first()
        self._create_opportunity(campaign)
        filters = dict(indicator=Indicator.CPV, breakdown=Breakdown.DAILY)
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: visible_all_accounts,
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings), \
             override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), expected_count)
