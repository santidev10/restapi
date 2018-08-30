from datetime import date, timedelta
from numbers import Number

from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class DashboardAccountCreationOverviewAPITestCase(ExtendedAPITestCase):
    _overview_keys = {
        "age",
        "all_conversions",
        "average_cpm",
        "average_cpv",
        "average_cpv_bottom",
        "average_cpv_top",
        "clicks",
        "clicks_last_week",
        "clicks_this_week",
        "conversions",
        "cost",
        "cost_last_week",
        "cost_this_week",
        "ctr",
        "ctr_bottom",
        "ctr_top",
        "ctr_v",
        "ctr_v_bottom",
        "ctr_v_top",
        "delivered_cost",
        "delivered_impressions",
        "delivered_video_views",
        "device",
        "gender",
        "has_statistics",
        "impressions",
        "impressions_last_week",
        "impressions_this_week",
        "location",
        "plan_cost",
        "plan_impressions",
        "plan_video_views",
        "video100rate",
        "video25rate",
        "video50rate",
        "video75rate",
        "video_clicks",
        "video_view_rate",
        "video_view_rate_bottom",
        "video_view_rate_top",
        "video_views",
        "video_views_last_week",
        "video_views_this_week",
        "view_through",
    }

    def _get_url(self, account_creation_id):
        return reverse(Name.Dashboard.ACCOUNT_OVERVIEW, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
                       args=(account_creation_id,))

    def _request(self, account_creation_id, status_code=HTTP_200_OK, **kwargs):
        url = self._get_url(account_creation_id)
        response = self.client.post(url, kwargs)
        self.assertEqual(response.status_code, status_code)
        return response.data

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_dashboard")

    def test_success(self):
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
        self.assertEqual(set(overview.keys()), self._overview_keys)

    def test_success_demo(self):
        overview = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(set(overview.keys()), self._overview_keys)

    def test_hidden_costs(self):
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        for rates_are_hidden in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = rates_are_hidden
            with self.subTest(**user_settings), self.patch_user_settings(**user_settings):
                overview = self._request(account.account_creation.id)
                self.assertNotIn("delivered_cost", overview)
                self.assertNotIn("plan_cost", overview)

    def test_visible_costs(self):
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        for rates_are_hidden in (True, False):
            user_settings[UserSettingsKey.DASHBOARD_AD_WORDS_RATES] = rates_are_hidden
            with self.subTest(**user_settings), self.patch_user_settings(**user_settings):
                overview = self._request(account.account_creation.id)
                self.assertIn("delivered_cost", overview)
                self.assertIn("plan_cost", overview)

    def test_plan_cost(self):
        account = Account.objects.create()
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
                overview = self._request(account.account_creation.id)
                self.assertEqual(overview["plan_cost"], total_cost)

    def test_delivered_cost_aw_cost(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
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
            overview = self._request(account.account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["delivered_cost"], aw_cost)

    def test_delivered_cost_client_cost_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
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
            overview = self._request(account.account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["delivered_cost"], client_cost)

    def test_delivered_impressions_cpm_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
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
                overview = self._request(account.account_creation.id, start_date=str(any_date), end_date=str(any_date))
                self.assertEqual(overview["delivered_impressions"], impressions[0])

    def test_delivered_impressions_cpv_ignored(self):
        any_date = date(2018, 1, 1)
        account = Account.objects.create()
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
                overview = self._request(account.account_creation.id)
                self.assertEqual(overview["delivered_impressions"], 0)

    def test_delivered_views_cpv_filtered_by_date(self):
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
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
                overview = self._request(account.account_creation.id, start_date=str(any_date), end_date=str(any_date))
                self.assertEqual(overview["delivered_video_views"], video_views[0])

    def test_delivered_views_cpm_ignored(self):
        any_date = date(2018, 1, 1)
        account = Account.objects.create()
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
                overview = self._request(account.account_creation.id)
                self.assertEqual(overview["delivered_video_views"], 0)

    def test_delivered_cost_is_zero(self):
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
            self.assertEqual(overview["delivered_cost"], 0)
            self.assertEqual(overview["plan_cost"], 0)

    def test_aw_cost(self):
        self.user.is_staff = True
        self.user.save()
        account = Account.objects.create()
        costs = (123, 234)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign_1 = Campaign.objects.create(
            id=1, account=account, cost=costs[0],
            salesforce_placement=placement)
        campaign_2 = Campaign.objects.create(
            id=2, account=account, cost=costs[1],
            salesforce_placement=placement)
        ad_group_1 = AdGroup.objects.create(
            id=1, campaign=campaign_1, cost=costs[0])
        ad_group_2 = AdGroup.objects.create(
            id=2, campaign=campaign_2, cost=costs[1])
        AdGroupStatistic.objects.create(
            date=date(2018, 1, 1), ad_group=ad_group_1,
            cost=costs[0], average_position=1)
        AdGroupStatistic.objects.create(
            date=date(2018, 1, 1), ad_group=ad_group_2,
            cost=costs[1], average_position=1)
        expected_cost = sum(costs)
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
        self.assertAlmostEqual(overview["delivered_cost"], expected_cost)

    def test_cost_client_cost(self):
        self.user.is_staff = True
        account = Account.objects.create()
        opportunity = Opportunity.objects.create()
        placement_cpm = OpPlacement.objects.create(
            id=1, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, ordered_rate=2.)
        placement_cpv = OpPlacement.objects.create(
            id=2, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, ordered_rate=2.)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=3, opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE)
        placement_hard_cost = OpPlacement.objects.create(
            id=4, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST, total_cost=523)
        placement_dynamic_budget = OpPlacement.objects.create(
            id=5, opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)
        placement_cpv_rate_and_tech_fee = OpPlacement.objects.create(
            id=6, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=.2)
        placement_cpm_rate_and_tech_fee = OpPlacement.objects.create(
            id=7, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=.3)
        campaigns = (
            Campaign.objects.create(
                id=1, account=account,
                salesforce_placement=placement_cpm, impressions=2323),
            Campaign.objects.create(
                id=2, account=account,
                salesforce_placement=placement_cpv, video_views=321),
            Campaign.objects.create(
                id=3, account=account,
                salesforce_placement=placement_outgoing_fee),
            Campaign.objects.create(
                id=4, account=account,
                salesforce_placement=placement_hard_cost),
            Campaign.objects.create(
                id=5, account=account,
                salesforce_placement=placement_dynamic_budget, cost=412),
            Campaign.objects.create(
                id=6, account=account,
                salesforce_placement=placement_cpv_rate_and_tech_fee,
                video_views=245, cost=32),
            Campaign.objects.create(
                id=7, account=account,
                salesforce_placement=placement_cpm_rate_and_tech_fee,
                impressions=632, cost=241))
        for index, campaign in enumerate(campaigns):
            ad_group = AdGroup.objects.create(id=index, campaign=campaign)
            AdGroupStatistic.objects.create(
                date=date(2018, 1, 1), ad_group=ad_group, average_position=1,
                cost=campaign.cost, impressions=campaign.impressions,
                video_views=campaign.video_views)
        expected_cost = sum(
            [get_client_cost(
                goal_type_id=c.salesforce_placement.goal_type_id,
                dynamic_placement=c.salesforce_placement.dynamic_placement,
                placement_type=c.salesforce_placement.placement_type,
                ordered_rate=c.salesforce_placement.ordered_rate,
                impressions=c.impressions,
                video_views=c.video_views,
                aw_cost=c.cost,
                total_cost=c.salesforce_placement.total_cost,
                tech_fee=c.salesforce_placement.tech_fee,
                start=c.start_date,
                end=c.end_date)
                for c in campaigns])
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
        self.assertAlmostEqual(overview["delivered_cost"], expected_cost)

    def test_hide_costs_according_to_user_settings(self):
        self.user.is_staff = True
        opportunity = Opportunity.objects.create()
        placement_cpm = OpPlacement.objects.create(
            id=1, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            ordered_units=1, total_cost=1)
        placement_cpv = OpPlacement.objects.create(
            id=2, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=1, total_cost=1)
        account = Account.objects.create()
        Campaign.objects.create(
            id=1, salesforce_placement=placement_cpm,
            account=account, cost=1, impressions=1)
        Campaign.objects.create(
            id=2, salesforce_placement=placement_cpv,
            account=account, cost=1, video_views=1)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
        self.assertIsNotNone(overview)
        self.assertIn("cost", overview)
        self.assertIn("average_cpm", overview)
        self.assertIn("average_cpv", overview)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
        self.assertIsNotNone(overview)
        self.assertNotIn("cost", overview)
        self.assertNotIn("average_cpm", overview)
        self.assertNotIn("average_cpv", overview)

    def test_campaigns_filter_affect_performance_data(self):
        user = self.user
        any_date = date(2018, 1, 1)
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        opportunity = Opportunity.objects.create(id=1)
        expected_plan_cost = 3
        expected_plan_impressions = 3
        placement_one = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            total_cost=expected_plan_cost, ordered_units=expected_plan_impressions)
        placement_two = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM, total_cost=5, ordered_units=5)
        campaign_one_id, campaign_two_id = 1, 2
        ad_group_one_id, ad_group_two_id = 3, 4
        campaign_one = Campaign.objects.create(
            id=campaign_one_id, salesforce_placement=placement_one, account=account)
        campaign_two = Campaign.objects.create(
            id=campaign_two_id, salesforce_placement=placement_two, account=account)
        expected_delivered_cost = 100
        expected_delivered_impressions = 50
        expected_delivered_video_views = 100
        ad_group_1 = AdGroup.objects.create(
            id=ad_group_one_id, campaign=campaign_one,
        )

        ad_group_2 = AdGroup.objects.create(
            id=ad_group_two_id, campaign=campaign_two, cost=1,
            video_views=1, impressions=1)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group_1, average_position=1,
                                        cost=expected_delivered_cost, video_views=expected_delivered_video_views,
                                        impressions=expected_delivered_impressions)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group_2, average_position=1, cost=1,
                                        video_views=1, impressions=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id, campaigns=[campaign_one_id])
        self.assertEqual(overview["plan_cost"], expected_plan_cost)
        self.assertEqual(overview["plan_impressions"], expected_plan_impressions)
        self.assertEqual(overview["delivered_cost"], expected_delivered_cost)
        self.assertEqual(overview["delivered_impressions"], expected_delivered_impressions)

    def test_overview_reflects_to_date_range(self):
        user = self.user
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        opportunity = Opportunity.objects.create(id=1)
        placement = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            total_cost=5, ordered_units=5)
        campaign = Campaign.objects.create(
            id=1, salesforce_placement=placement,
            account=account)
        cost_1, cost_2 = 3, 4
        impressions_1, impressions_2 = 3, 4
        views_1, views_2 = 3, 4
        ad_group = AdGroup.objects.create(
            id=1, campaign=campaign,
            cost=cost_1 + cost_2, video_views=views_1 + views_2,
            impressions=impressions_1 + impressions_2)
        date_1 = date(2018, 7, 1)
        date_2 = date_1 + timedelta(days=1)
        AdGroupStatistic.objects.create(date=date_1, ad_group=ad_group,
                                        average_position=1,
                                        cost=cost_1, impressions=impressions_1,
                                        video_views=views_1)
        AdGroupStatistic.objects.create(date=date_2, ad_group=ad_group,
                                        average_position=1,
                                        cost=cost_2, impressions=impressions_2,
                                        video_views=views_2)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id,
                                     start_date=str(date_1),
                                     end_date=str(date_1))
        self.assertEqual(overview["cost"], cost_1)
        self.assertEqual(overview["impressions"], impressions_1)
        self.assertEqual(overview["video_views"], views_1)

    def test_ad_groups_filter_affect_performance_data(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        opportunity = Opportunity.objects.create(id=1)
        expected_plan_cost = 3
        expected_plan_impressions = 3
        placement_one = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            total_cost=expected_plan_cost,
            ordered_units=expected_plan_impressions)
        placement_two = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            total_cost=5, ordered_units=5)
        campaign_one_id, campaign_two_id = 1, 2
        ad_group_one_id, ad_group_two_id = 3, 4
        campaign_one = Campaign.objects.create(
            id=campaign_one_id, salesforce_placement=placement_one,
            account=account)
        campaign_two = Campaign.objects.create(
            id=campaign_two_id, salesforce_placement=placement_two,
            account=account)
        expected_delivered_cost = 100
        expected_delivered_impressions = 50
        expected_delivered_video_views = 100
        ad_group_1 = AdGroup.objects.create(
            id=ad_group_one_id, campaign=campaign_one)
        ad_group_2 = AdGroup.objects.create(
            id=ad_group_two_id, campaign=campaign_two)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group_1, average_position=1,
                                        cost=expected_delivered_cost,
                                        video_views=expected_delivered_video_views,
                                        impressions=expected_delivered_impressions)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group_2, average_position=1,
                                        cost=1, video_views=1, impressions=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id,
                                     ad_groups=[ad_group_one_id])
        self.assertEqual(overview["plan_cost"], expected_plan_cost)
        self.assertEqual(overview["plan_impressions"], expected_plan_impressions)
        self.assertEqual(overview["delivered_cost"], expected_delivered_cost)
        self.assertEqual(overview["delivered_impressions"], expected_delivered_impressions)

    def test_conversions_are_hidden(self):
        self.create_test_user()
        any_date = date(2018, 1, 1)
        conversions = 2
        all_conversions = 3
        view_through = 4
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign, conversions=2,
                                          all_conversions=3, view_through=4)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=any_date, average_position=1,
                                        conversions=conversions,
                                        all_conversions=all_conversions,
                                        view_through=view_through)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: False,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
            self.assertNotIn("conversions", overview)
            self.assertNotIn("all_conversions", overview)
            self.assertNotIn("view_through", overview)

    def test_conversions_are_visible(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        conversions = 2
        all_conversions = 3
        view_through = 4
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=any_date, average_position=1,
                                        conversions=conversions,
                                        all_conversions=all_conversions,
                                        view_through=view_through)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account.account_creation.id)
            self.assertEqual(overview["conversions"], conversions)
            self.assertEqual(overview["all_conversions"], all_conversions)
            self.assertEqual(overview["view_through"], view_through)

    def test_demo_account_performance_charts(self):
        overview = self._request(DEMO_ACCOUNT_ID)
        keys = (
            "delivered_cost",
            "delivered_impressions",
            "delivered_video_views",
            "plan_cost",
            "plan_impressions",
            "plan_video_views",
        )
        for key in keys:
            self.assertIn(key, overview, key)
            self.assertIsInstance(overview[key], Number, key)
        self.assertGreater(overview["plan_cost"], overview["delivered_cost"])
        self.assertGreater(overview["plan_impressions"], overview["delivered_impressions"])
        self.assertGreater(overview["plan_video_views"], overview["delivered_video_views"])

    def test_demo_data_are_equal_to_header(self):
        data = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(data["delivered_impressions"], 150000)
        self.assertEqual(data["delivered_video_views"], 53000)
        self.assertEqual(data["delivered_cost"], 3700)

    def test_demo_data_filtered_by_campaign(self):
        base_data = self._request(DEMO_ACCOUNT_ID)

        data = self._request(DEMO_ACCOUNT_ID, campaigns=["demo1"])
        self.assertGreater(base_data["delivered_impressions"], data["delivered_impressions"])
        self.assertGreater(base_data["delivered_video_views"], data["delivered_video_views"])
        self.assertGreater(base_data["delivered_cost"], data["delivered_cost"])

    def test_demo_data_filtered_by_ad_groups(self):
        base_data = self._request(DEMO_ACCOUNT_ID)

        data = self._request(DEMO_ACCOUNT_ID, ad_groups=["demo11"])
        self.assertGreater(base_data["delivered_impressions"], data["delivered_impressions"])
        self.assertGreater(base_data["delivered_video_views"], data["delivered_video_views"])
        self.assertGreater(base_data["delivered_cost"], data["delivered_cost"])
