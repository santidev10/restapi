import json
from datetime import date

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import int_iterator


class DashboardAccountOverviewAPITestCase(ExtendedAPITestCase):

    def _get_url(self, account_creation_id):
        return reverse(
            RootNamespace.AW_CREATION + ":" + Namespace.DASHBOARD + ":" + Name.Dashboard.ACCOUNT_OVERVIEW,
            args=(account_creation_id,))

    def _request(self, account_creation_id, status_code=HTTP_200_OK, **kwargs):
        url = self._get_url(account_creation_id)
        response = self.client.post(url, json.dumps(dict(is_chf=1, **kwargs)), content_type="application/json")
        self.assertEqual(response.status_code, status_code)
        return response.data

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_dashboard")

    def test_hidden_costs(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        for rates_are_hidden in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = rates_are_hidden
            with self.subTest(**user_settings), self.patch_user_settings(**user_settings):
                overview = self._request(account_creation.id)
                self.assertNotIn("delivered_cost", overview)
                self.assertNotIn("plan_cost", overview)

    def test_visible_costs(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        for rates_are_hidden in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = rates_are_hidden
            with self.subTest(**user_settings), self.patch_user_settings(**user_settings):
                overview = self._request(account_creation.id)
                self.assertIn("delivered_cost", overview)
                self.assertIn("plan_cost", overview)

    def test_plan_cost(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        total_cost = 123
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity, total_cost=total_cost)
        Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        for rates_are_hidden in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = rates_are_hidden
            with self.patch_user_settings(**user_settings):
                overview = self._request(account_creation.id)
                self.assertEqual(overview["plan_cost"], total_cost)

    def test_delivered_cost_aw_cost(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        aw_cost = 123
        aw_cost_irrelevant = 23
        self.assertGreater(aw_cost_irrelevant, 0)
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity)
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, cost=aw_cost, average_position=1)
        AdGroupStatistic.objects.create(date=another_date, ad_group=ad_group, cost=aw_cost_irrelevant,
                                        average_position=1)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["delivered_cost"], aw_cost)

    def test_delivered_cost_client_cost_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        aw_cost = 123, 23
        views = 234, 345
        impressions = 2345, 3456
        self.assertGreater(aw_cost[1], 0)
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity, ordered_rate=5.)
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, cost=aw_cost[0], average_position=1,
                                        video_views=views[0], impressions=impressions[0])
        AdGroupStatistic.objects.create(date=another_date, ad_group=ad_group, cost=aw_cost[1], average_position=1,
                                        video_views=views[1], impressions=impressions[1])
        client_cost = get_client_cost(
            goal_type_id=placement.goal_type_id,
            dynamic_placement=placement.goal_type_id,
            placement_type=placement.placement_type,
            ordered_rate=placement.ordered_rate,
            impressions=impressions[0],
            video_views=views[0],
            aw_cost=aw_cost[0],
            total_cost=placement.total_cost,
            tech_fee=placement.tech_fee,
            start=None,
            end=None
        )
        self.assertGreater(client_cost, 0)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["delivered_cost"], client_cost)

    def test_delivered_impressions_cpm_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        impressions = 2345, 3456
        placement = OpPlacement.objects.create(id=next(int_iterator), goal_type_id=SalesForceGoalType.CPM,
                                               opportunity=opportunity,
                                               ordered_rate=5., )
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        impressions=impressions[0])
        AdGroupStatistic.objects.create(date=another_date, ad_group=ad_group, average_position=1,
                                        impressions=impressions[1])
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        for dashboard_ad_words_rates in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = dashboard_ad_words_rates
            with self.patch_user_settings(**user_settings), \
                 self.subTest(**user_settings):
                overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
                self.assertEqual(overview["delivered_impressions"], impressions[0])

    def test_delivered_impressions_cpv_ignored(self):
        any_date = date(2018, 1, 1)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        impressions = 2345
        placement = OpPlacement.objects.create(id=next(int_iterator), goal_type_id=SalesForceGoalType.CPV,
                                               opportunity=opportunity,
                                               ordered_rate=5., )
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1, impressions=impressions)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        for dashboard_ad_words_rates in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = dashboard_ad_words_rates
            with self.patch_user_settings(**user_settings), \
                 self.subTest(**user_settings):
                overview = self._request(account_creation.id)
                self.assertEqual(overview["delivered_impressions"], 0)

    def test_delivered_views_cpv_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        video_views = 2345, 3456
        placement = OpPlacement.objects.create(id=next(int_iterator), goal_type_id=SalesForceGoalType.CPV,
                                               opportunity=opportunity,
                                               ordered_rate=5., )
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        video_views=video_views[0])
        AdGroupStatistic.objects.create(date=another_date, ad_group=ad_group, average_position=1,
                                        video_views=video_views[1])
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        for dashboard_ad_words_rates in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = dashboard_ad_words_rates
            with self.patch_user_settings(**user_settings), \
                 self.subTest(**user_settings):
                overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
                self.assertEqual(overview["delivered_video_views"], video_views[0])

    def test_delivered_views_cpm_ignored(self):
        any_date = date(2018, 1, 1)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        video_views = 2345
        placement = OpPlacement.objects.create(id=next(int_iterator), goal_type_id=SalesForceGoalType.CPM,
                                               opportunity=opportunity,
                                               ordered_rate=5., )
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1, video_views=video_views)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        for dashboard_ad_words_rates in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = dashboard_ad_words_rates
            with self.patch_user_settings(**user_settings), \
                 self.subTest(**user_settings):
                overview = self._request(account_creation.id)
                self.assertEqual(overview["delivered_video_views"], 0)

    def test_delivered_cost_is_zero(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(id=next(int_iterator),
                                                          account=account,
                                                          owner=self.request_user,
                                                          is_approved=True)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id)
            self.assertEqual(overview["delivered_cost"], 0)
            self.assertEqual(overview["plan_cost"], 0)
