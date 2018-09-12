import json
from datetime import date
from itertools import product

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.dashboard_charts import ALL_DIMENSIONS
from aw_reporting.dashboard_charts import ALL_INDICATORS
from aw_reporting.dashboard_charts import Indicator
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase, reverse
from utils.utils_tests import int_iterator


class DashboardPerformanceChartTestCase(ExtendedAPITestCase):
    def _request(self, account_creation_id, **kwargs):
        url = reverse(
            Name.Dashboard.PERFORMANCE_CHART,
            [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
            args=(account_creation_id,)
        )
        return self.client.post(url,
                                json.dumps(dict(is_staff=False, **kwargs)),
                                content_type="application/json")

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

    def create_test_user(self, auth=True):
        user = super(DashboardPerformanceChartTestCase, self).create_test_user(
            auth)
        user.add_custom_user_permission("view_dashboard")
        return user

    def test_success_on_no_global_account_visibility(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1)
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id,
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
        self.create_test_user()
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=12)
        account = Account.objects.create(id=next(int_iterator))
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
            with self.subTest(show_ad_words_rate=ad_words_rate), \
                 self.patch_user_settings(**user_settings):

                response = self._request(account.account_creation.id,
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
