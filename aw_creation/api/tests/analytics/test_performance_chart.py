import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import product

from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.analytics_charts import ALL_DIMENSIONS
from aw_reporting.analytics_charts import Dimension
from aw_reporting.analytics_charts import Indicator
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse


class AnalyticsPerformanceChartTestCase(ExtendedAPITestCase):
    def _request(self, account_creation_id, **kwargs):
        url = reverse(
            Name.Analytics.PERFORMANCE_CHART,
            [RootNamespace.AW_CREATION, Namespace.ANALYTICS],
            args=(account_creation_id,)
        )
        return self.client.post(
            url,
            json.dumps(dict(is_staff=False, **kwargs)),
            content_type="application/json"
        )

    def _hide_demo_data(self, user):
        self._create_manager_with_connection(user)

    def _create_manager_with_connection(self, user):
        connection = AWConnection.objects.create(
            email="me@mail.kz",
            refresh_token="")
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=user,
        )
        manager = Account.objects.create(id=next(int_iterator), can_manage_clients=True)
        AWAccountPermission.objects.create(account=manager,
                                           aw_connection=connection)
        return manager

    @staticmethod
    def create_stats(account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1,
                                                     defaults=dict(name="boo",
                                                                   type="A"))
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(
            name="Babruysk"))
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
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        today = datetime.now().date()
        response = self._request(account_creation.id,
                                 start_date=str(today - timedelta(days=2)),
                                 end_date=str(today - timedelta(days=1)),
                                 indicator=Indicator.IMPRESSIONS)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(data[1]['title'], "#1")
        self.assertEqual(data[2]['title'], "#2")

    def test_success_get_filter_items(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }

        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id,
                                     campaigns=["1"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], "#1")
        self.assertEqual(len(data[0]['data'][0]['trend']), 1)

    @generic_test([
        (dimension, (dimension,), dict())
        for dimension in (Dimension.DEVICE, Dimension.GENDER, Dimension.AGE, Dimension.TOPIC, Dimension.INTEREST,
                          Dimension.CREATIVE, Dimension.CHANNEL, Dimension.VIDEO, Dimension.KEYWORD,
                          Dimension.LOCATION, Dimension.ADS)
    ])
    def test_all_dimensions(self, dimension):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        filters = {
            'indicator': 'video_view_rate',
            'dimension': dimension,
        }
        response = self._request(account_creation.id, **filters)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(len(response.data[0]['data']), 1)

    def test_success_get_no_account(self):
        user = self.create_test_user()
        self._hide_demo_data(user)

        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          sync_at=timezone.now())

        account = Account.objects.create(id=1, name="")
        self.create_stats(account)  # create stats that won't be visible

        today = datetime.now().date()
        response = self._request(account_creation.id,
                                 start_date=str(today - timedelta(days=2)),
                                 end_date=str(today - timedelta(days=1)),
                                 indicator=Indicator.IMPRESSIONS)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_success_demo(self):
        recreate_demo_data()
        self.create_test_user()

        today = datetime.now().date()
        response = self._request(DEMO_ACCOUNT_ID,
                                 start_date=str(today - timedelta(days=2)),
                                 end_date=str(today - timedelta(days=1)),
                                 indicator=Indicator.IMPRESSIONS)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(len(data[0]['data'][0]['trend']), 2)
        self.assertEqual(data[1]['title'], "Campaign #demo1")
        self.assertEqual(data[2]['title'], "Campaign #demo2")

    def test_success_demo_data(self):
        recreate_demo_data()
        user = self.create_test_user()

        today = datetime.now().date()
        response = self._request(DEMO_ACCOUNT_ID,
                                 start_date=str(today - timedelta(days=2)),
                                 end_date=str(today - timedelta(days=1)),
                                 indicator=Indicator.IMPRESSIONS)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(len(data[0]['data'][0]['trend']), 2)
        self.assertEqual(data[1]['title'], "Campaign #demo1")
        self.assertEqual(data[2]['title'], "Campaign #demo2")

    def test_cpm_cpv_is_visible(self):
        recreate_demo_data()
        user = self.create_test_user()

        account_creation_id = DEMO_ACCOUNT_ID
        for args in product(
                (True, False),
                (True, False),
                (Indicator.CPM, Indicator.CPV),
                ALL_DIMENSIONS,
                (True, False),
                (True, False)
        ):
            msg = "AW cost = {}, hide dashboard cost = {}, indicator = {}, dimension = {}, " \
                  "is_demo = {}, is_staff = {}".format(*args)
            dashboard_ad_words_rates, hide_dashboard_cost, indicator, dimension, is_demo, is_staff = args
            user.is_staff = is_staff
            user.save()
            if not is_demo:
                account_creation_id = AccountCreation.objects.create(name="", owner=user,
                                                                     is_paused=True).id
            user_settings = {
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: dashboard_ad_words_rates,
                UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: hide_dashboard_cost,
            }

            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg):
                response = self._request(account_creation_id,
                                         indicator=indicator,
                                         dimention=dimension)
                self.assertEqual(response.status_code, HTTP_200_OK)

    def test_cost_does_not_reflect_to_aw_rates_setting(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=12)
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True,
                                                          account=account)
        campaign = Campaign.objects.create(id=next(int_iterator),
                                           salesforce_placement=placement,
                                           account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator),
                                          campaign=campaign)
        impressions, views, aw_cost = 500, 200, 30
        AdGroupStatistic.objects.create(ad_group=ad_group,
                                        average_position=1,
                                        date=any_date,
                                        video_views=views,
                                        cost=aw_cost)

        client_cost = get_client_cost(
            goal_type_id=placement.goal_type_id,
            dynamic_placement=placement.dynamic_placement,
            placement_type=placement.placement_type,
            ordered_rate=placement.ordered_rate,
            impressions=None,
            video_views=views,
            aw_cost=aw_cost,
            total_cost=placement.total_cost,
            tech_fee=placement.tech_fee,
            start=None,
            end=None
        )
        self.assertNotAlmostEqual(aw_cost, client_cost)
        test_cases = (False, True)
        for ad_words_rate in test_cases:
            user_settings = {
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: ad_words_rate,
                UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
            }
            with self.subTest(show_ad_words_rate=ad_words_rate), \
                 self.patch_user_settings(**user_settings):

                response = self._request(account_creation.id,
                                         indicator=Indicator.COST)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(len(response.data), 1)
                chart_data = response.data[0]["data"]
                self.assertEqual(len(chart_data), 1)
                trend = chart_data[0]["trend"]
                self.assertEqual(len(trend), 1)
                trend_item = trend[0]
                self.assertEqual(trend_item["label"], any_date)
                self.assertAlmostEqual(trend_item["value"], aw_cost)

    def test_account_visibility_independent(self):
        any_indicator = Indicator.CPV
        any_dimension = Dimension.ADS
        user = self.create_test_user()
        manager = self._create_manager_with_connection(user)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        self.create_stats(account)
        account_creation = account.account_creation

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }

        with self.patch_user_settings(**user_settings):
            response = self._request(
                account_creation.id,
                indicator=any_indicator,
                dimension=any_dimension
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
