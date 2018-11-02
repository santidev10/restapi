import json
from datetime import date
from itertools import product
from unittest.mock import patch

from django.conf import settings
from django.db.models import Sum
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import CampaignCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWConnection, SFAccount
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class DashboardAccountCreationDetailsAPITestCase(ExtendedAPITestCase):
    _keys = {
        "account",
        "ad_count",
        "average_cpm",
        "average_cpv",
        "brand",
        "channel_count",
        "clicks",
        "clicks_app_store",
        "clicks_call_to_action_overlay",
        "clicks_cards",
        "clicks_end_cap",
        "clicks_website",
        "cost",
        "cost_method",
        "ctr",
        "ctr_v",
        "details",
        "end",
        "id",
        "impressions",
        "interest_count",
        "is_changed",
        "is_disapproved",
        "keyword_count",
        "name",
        "plan_cpm",
        "plan_cpv",
        "sf_account",
        "start",
        "thumbnail",
        "topic_count",
        "updated_at",
        "video_count",
        "video_view_rate",
        "video_views",
        "weekly_chart",
    }

    def _get_url(self, account_creation_id):
        return reverse(Name.Dashboard.ACCOUNT_DETAILS, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
                       args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(kwargs), content_type="application/json")

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_dashboard")

    def test_properties(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), self._keys)

    def test_properties_demo(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        response = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), self._keys)

    def test_demo_details_for_chf_acc(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        response = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], None)

    def test_average_cpm_and_cpv_reflects_to_user_settings(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertNotIn("average_cpm", response.data)
        self.assertNotIn("average_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertIn("average_cpm", response.data)
        self.assertIn("average_cpv", response.data)

    def test_plan_cpm_and_cpv_reflects_to_user_settings(self):
        account = Account.objects.create()
        account_creation_id = account.account_creation.id
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation_id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation_id)
        self.assertNotIn("plan_cpm", response.data)
        self.assertNotIn("plan_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation_id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation_id)
        self.assertIn("plan_cpm", response.data)
        self.assertIn("plan_cpv", response.data)

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
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)

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
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)

    def test_hide_costs_according_to_user_settings(self):
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
        account_creation_id = account.account_creation.id
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
            response = self._request(account_creation_id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        acc_data = response.data
        self.assertIsNotNone(acc_data)
        self.assertIn("cost", acc_data)
        self.assertIn("plan_cpm", acc_data)
        self.assertIn("plan_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)
        self.assertIn("average_cpv", acc_data)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation_id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        acc_data = response.data
        self.assertIsNotNone(acc_data)
        self.assertNotIn("cost", acc_data)
        self.assertNotIn("plan_cpm", acc_data)
        self.assertNotIn("plan_cpv", acc_data)
        self.assertNotIn("average_cpm", acc_data)
        self.assertNotIn("average_cpv", acc_data)

    def test_brand(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="2", name="")
        managed_account.managers.add(chf_account)
        test_brand = "Test Brand"
        opportunity = Opportunity.objects.create(brand=test_brand)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(managed_account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["brand"], test_brand)

    def test_success_get_chf_account(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        managed_account.managers.add(chf_account)
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [managed_account.id]
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self._request(managed_account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_sf_account(self):
        sf_account = SFAccount.objects.create(name="test account")
        opportunity = Opportunity.objects.create(account=sf_account)
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        managed_account.managers.add(chf_account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(managed_account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["sf_account"], sf_account.name)

    def test_cost_method(self):
        opportunity = Opportunity.objects.create()
        placement1 = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM)
        placement2 = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV)
        placement3 = OpPlacement.objects.create(
            id=3, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        managed_account.managers.add(chf_account)
        Campaign.objects.create(
            id="1", salesforce_placement=placement1, account=managed_account)
        Campaign.objects.create(
            id="2", salesforce_placement=placement2, account=managed_account)
        Campaign.objects.create(
            id="3", salesforce_placement=placement3, account=managed_account)
        account_creation = managed_account.account_creation
        CampaignCreation.objects.create(account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(account_creation=account_creation, campaign=None)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_dynamic_placement_budget_rates_are_empty(self):
        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            ordered_units=1000, ordered_rate=1.2)
        placement_cpm = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.BUDGET,
            ordered_units=1000, ordered_rate=1.3)

        Flight.objects.create(id=next(int_iterator), placement=placement_cpm,
                              total_cost=1,
                              ordered_units=1)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv,
                              total_cost=1,
                              ordered_units=1)
        account = Account.objects.create(id=next(int_iterator))
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpm,
                                account=account)
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpv,
                                account=account)
        account_creation_id = account.account_creation.id
        url = self._get_url(account_creation_id)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation_id)
        with self.subTest("CPM"):
            self.assertIsNone(response.data["plan_cpm"])
        with self.subTest("CPV"):
            self.assertIsNone(response.data["plan_cpv"])

    def test_dashboard_planned_cpv_and_cpm_are_none(self):
        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(
            id=1,
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_units=1000, ordered_rate=1.2, total_cost=30)
        placement_cpm = OpPlacement.objects.create(
            id=2,
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_units=1000, ordered_rate=1.3, total_cost=40)
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=self.request_user)
        account = Account.objects.create()
        account_creation_id = account.account_creation.id
        Campaign.objects.create(id=1, salesforce_placement=placement_cpm,
                                account=account)
        Campaign.objects.create(id=2, salesforce_placement=placement_cpv,
                                account=account)

        plan_cpm = placement_cpm.total_cost / placement_cpm.ordered_units * 1000
        plan_cpv = placement_cpv.total_cost / placement_cpv.ordered_units
        costs_hidden_cases = (True, False)
        ad_words_rates_cases = (True, False)
        msg_keys = (
            ("CPM", "plan_cpm", plan_cpm),
            ("CPV", "plan_cpv", plan_cpv),
        )
        test_cases = product(costs_hidden_cases, ad_words_rates_cases, msg_keys)
        for cost_hidden, aw_rate, msg_key_value in test_cases:
            msg, key, expected_value = msg_key_value
            user_settings = {
                UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: cost_hidden,
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rate,
                UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
            }

            with self.subTest(msg, **user_settings), \
                 self.patch_user_settings(**user_settings):
                response = self._request(account_creation_id)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(response.data["id"], account_creation_id)
                actual_value = response.data.get(key)
                if cost_hidden:
                    self.assertIsNone(actual_value)
                else:
                    self.assertIsNotNone(actual_value)
                    self.assertAlmostEqual(actual_value, expected_value)

    def test_average_cpm_and_cpv_not_reflect_to_user_settings(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings), \
             self.subTest("hide"):
            response = self._request(account_creation.id)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["id"], account_creation.id)
            self.assertNotIn("average_cpm", response.data)
            self.assertNotIn("average_cpv", response.data)
            self.assertNotIn("plan_cpm", response.data)
            self.assertNotIn("plan_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
        with self.patch_user_settings(**user_settings), \
             self.subTest("show"):
            response = self._request(account_creation.id)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["id"], account_creation.id)
            self.assertIn("average_cpm", response.data)
            self.assertIn("average_cpv", response.data)
            self.assertIn("plan_cpm", response.data)
            self.assertIn("plan_cpv", response.data)

    def test_no_demo_data(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(chf_mcc_account)
        account.save()
        Campaign.objects.create(id=next(int_iterator), account=account)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)

        self.assertEqual(response.status_code, HTTP_200_OK)
        item = response.data
        stats = (
            "clicks",
            "cost",
            "impressions",
            "video_views",
        )
        rates = (
            "average_cpm",
            "average_cpv",
            "ctr",
            "ctr_v",
            "plan_cpm",
            "plan_cpv",
            "video_view_rate",
        )
        for key in stats:
            self.assertEqual(item[key], 0, key)
        for key in rates:
            self.assertIsNone(item[key])

    def test_rates_on_multiple_campaigns(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/VIQ-278
        Summary: Dashboard > Incorrect cpv/ cpm on Dashboard for Dynamic placement if several placements with the same type are present
        Root cause: stats aggregates multiple times on several Campaign-Placement relations
        """
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(chf_mcc_account)
        account.save()
        opportunity = Opportunity.objects.create()
        placement_cpm_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                     goal_type_id=SalesForceGoalType.CPM)
        placement_cpm_2 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                     goal_type_id=SalesForceGoalType.CPM)
        placement_cpv_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                     goal_type_id=SalesForceGoalType.CPV)
        placement_cpv_2 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                     goal_type_id=SalesForceGoalType.CPV)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm_1, total_cost=2, ordered_units=1)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm_2, total_cost=3, ordered_units=2)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv_1, total_cost=4, ordered_units=3)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv_2, total_cost=5, ordered_units=4)

        for index, placement in enumerate(opportunity.placements.all()):
            for _ in range(1 + index):
                Campaign.objects.create(id=next(int_iterator), account=account, salesforce_placement=placement)

        def get_agg(goal_type_id):
            return Flight.objects.filter(placement__opportunity=opportunity,
                                         placement__goal_type_id=goal_type_id) \
                .aggregate(cost=Sum("total_cost"),
                           units=Sum("ordered_units"))

        cpv_agg = get_agg(SalesForceGoalType.CPV)
        cpm_agg = get_agg(SalesForceGoalType.CPM)
        expected_cpm = cpm_agg["cost"] / cpm_agg["units"] * 1000
        expected_cpv = cpv_agg["cost"] / cpv_agg["units"]
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)

        self.assertEqual(response.status_code, HTTP_200_OK)
        item = response.data
        with self.subTest("CPM"):
            self.assertAlmostEqual(item["average_cpm"], expected_cpm)
        with self.subTest("CPV"):
            self.assertAlmostEqual(item["average_cpv"], expected_cpv)
