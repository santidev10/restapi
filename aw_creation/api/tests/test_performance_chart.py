import json
from datetime import datetime, timedelta, date
from itertools import product
from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.charts import Indicator, ALL_DIMENSIONS, ALL_INDICATORS
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    GenderStatistic, AgeRangeStatistic, AudienceStatistic, \
    VideoCreativeStatistic, YTVideoStatistic, YTChannelStatistic, \
    TopicStatistic, KeywordStatistic, CityStatistic, AdStatistic, \
    VideoCreative, GeoTarget, Audience, Topic, Ad, AWConnection, \
    AWConnectionToUserRelation, Opportunity, SalesForceGoalType, OpPlacement
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase, int_iterator
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class PerformanceChartAnalyticsTestCase(ExtendedAPITestCase):
    def _request(self, account_creation_id, **kwargs):
        url = reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.PERFORMANCE_CHART,
            args=(account_creation_id,))
        return self.client.post(url,
                                json.dumps(dict(is_staff=False, **kwargs)),
                                content_type="application/json")

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

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
            name="bobruisk"))
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
        account = Account.objects.create(id=1, name="")
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
        account = Account.objects.create(id=1, name="")
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

    def test_all_dimensions(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        filters = {
            'indicator': 'video_view_rate',
        }
        for dimension in ('device', 'gender', 'age', 'topic',
                          'interest', 'creative', 'channel', 'video',
                          'keyword', 'location', 'ad'):
            filters['dimension'] = dimension
            with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher):
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
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)

        today = datetime.now().date()
        response = self._request(account_creation.id,
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

    def test_hide_cost_based_on_user_settings(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)
        self.assertNotEqual(account_creation.status,
                            AccountCreation.STATUS_PENDING)

        dimensions = ALL_DIMENSIONS
        cost_indicators = Indicator.CPM, Indicator.CPV, Indicator.COST
        account_ids = account_creation.id, DEMO_ACCOUNT_ID

        test_cases = list(product(dimensions, cost_indicators, account_ids))

        def test_availability(expected_status_code):
            for dimension, indicator, account_id in test_cases:
                response = self._request(account_id,
                                         indicator=indicator,
                                         dimention=dimension)
                is_demo = account_id == DEMO_ACCOUNT_ID
                msg = "Demo: {}; Dimension: {}; Indicator: {}".format(
                    is_demo, dimension, indicator)
                self.assertEqual(response.status_code, expected_status_code,
                                 msg)

        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
        with self.patch_user_settings(**user_settings):
            test_availability(HTTP_200_OK)

        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            test_availability(HTTP_404_NOT_FOUND)

    def test_cpm_cpv_is_visible(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }

        indicators = Indicator.CPM, Indicator.CPV
        dimensions = ALL_DIMENSIONS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID
        staffs = True, False

        test_data = list(product(indicators, dimensions, account_ids, staffs))
        for indicator, dimension, account_id, is_staff in test_data:
            msg = "Indicator: {}, dimension: {}, account: {}, is_staff: {}" \
                  "".format(indicator, dimension, account_id, is_staff)
            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg=msg):
                user.is_staff = is_staff
                user.save()
                response = self._request(account_id,
                                         indicator=indicator,
                                         dimention=dimension)
                self.assertEqual(response.status_code, HTTP_200_OK)

    def test_cpm_cpv_is_not_visible(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }

        indicators = Indicator.CPM, Indicator.CPV
        dimensions = ALL_DIMENSIONS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID
        staffs = True, False

        test_data = list(product(indicators, dimensions, account_ids, staffs))
        for indicator, dimension, account_id, is_staff in test_data:
            msg = "Indicator: {}, dimension: {}, account: {}, is_staff: {}" \
                  "".format(indicator, dimension, account_id, is_staff)
            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg=msg):
                user.is_staff = is_staff
                user.save()
                response = self._request(account_id,
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
        account = Account.objects.create(id=next(int_iterator))
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
            with self.subTest(show_ad_words_rate=ad_words_rate),\
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


class PerformanceChartDashboardTestCase(ExtendedAPITestCase):
    def _request(self, account_creation_id, **kwargs):
        url = reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.PERFORMANCE_CHART,
            args=(account_creation_id,))
        return self.client.post(url,
                                json.dumps(dict(is_chf=1, **kwargs)),
                                content_type="application/json")

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

    def create_test_user(self, auth=True):
        user = super(PerformanceChartDashboardTestCase, self).create_test_user(
            auth)
        user.add_custom_user_permission("view_dashboard")
        return user

    def test_success_on_no_global_account_visibility(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(id=2,
                                                          name="", owner=user,
                                                          is_paused=True,
                                                          account=account)
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id,
                                     indicator=Indicator.CPV)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_tabs(self):
        user = self.create_test_user()
        user.is_staff = True
        self._hide_demo_data(user)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)
        self.assertNotEqual(account_creation.status,
                            AccountCreation.STATUS_PENDING)

        dimensions = ALL_DIMENSIONS
        indicators = ALL_INDICATORS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID

        test_cases = list(product(dimensions, indicators, account_ids))

        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        for dimension, indicator, account_id in test_cases:
            is_demo = account_id == DEMO_ACCOUNT_ID
            msg = "Demo: {}; Dimension: {}; Indicator: {}".format(
                is_demo, dimension, indicator)
            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg=msg):
                response = self._request(account_id,
                                         indicator=indicator,
                                         dimention=dimension)
                self.assertEqual(response.status_code, HTTP_200_OK)

    def test_cpm_cpv_is_visible(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }

        indicators = Indicator.CPM, Indicator.CPV
        dimensions = ALL_DIMENSIONS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID
        staffs = True, False

        test_data = list(product(indicators, dimensions, account_ids, staffs))
        for indicator, dimension, account_id, is_staff in test_data:
            msg = "Indicator: {}, dimension: {}, account: {}, is_staff: {}" \
                  "".format(indicator, dimension, account_id, is_staff)
            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg=msg):
                user.is_staff = is_staff
                user.save()
                response = self._request(account_id,
                                         indicator=indicator,
                                         dimention=dimension)
                self.assertEqual(response.status_code, HTTP_200_OK)

    def test_cpm_cpv_is_not_visible(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }

        indicators = Indicator.CPM, Indicator.CPV
        dimensions = ALL_DIMENSIONS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID
        staffs = True, False

        test_data = list(product(indicators, dimensions, account_ids, staffs))
        for indicator, dimension, account_id, is_staff in test_data:
            msg = "Indicator: {}, dimension: {}, account: {}, is_staff: {}" \
                  "".format(indicator, dimension, account_id, is_staff)
            with self.patch_user_settings(**user_settings), \
                 self.subTest(msg=msg):
                user.is_staff = is_staff
                user.save()
                response = self._request(account_id,
                                         indicator=indicator,
                                         dimention=dimension)
                self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_cost_reflects_to_aw_rates_setting(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=12)
        account = Account.objects.create(id=next(int_iterator))
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
        test_cases = (
            (False, client_cost),
            (True, aw_cost),
        )
        for ad_words_rate, expected_cost in test_cases:
            user_settings = {
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: ad_words_rate,
                UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
            }
            with self.subTest(show_ad_words_rate=ad_words_rate),\
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
                self.assertAlmostEqual(trend_item["value"], expected_cost)
