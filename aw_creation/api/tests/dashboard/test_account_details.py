import json
from datetime import date
from datetime import timedelta
from itertools import product
from unittest.mock import patch

from django.conf import settings
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import Contact
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import int_iterator


class DashboardAccountDetailsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            RootNamespace.AW_CREATION + ":" + Namespace.DASHBOARD + ":" + Name.Dashboard.ACCOUNT_DETAILS,
            args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(dict(is_chf=1, **kwargs)), content_type="application/json")

    def setUp(self):
        self.user = self.create_test_user()

    def test_details_for_chf_acc(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        response = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], None)

    def test_average_cpm_and_cpv_reflects_to_user_settings(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""),
            user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user, is_approved=True)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        self.user.add_custom_user_permission("view_dashboard")
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
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        self.user.add_custom_user_permission("view_dashboard")
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertNotIn("plan_cpm", response.data)
        self.assertNotIn("plan_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertIn("plan_cpm", response.data)
        self.assertIn("plan_cpv", response.data)

    def test_aw_cost(self):
        self.user.is_staff = True
        self.user.save()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
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
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)
        self.assertAlmostEqual(
            response.data["overview"]["delivered_cost"], expected_cost)

    def test_cost_client_cost(self):
        self.user.is_staff = True
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account,
            is_approved=True)
        account_creation.refresh_from_db()
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
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)
        self.assertAlmostEqual(
            response.data["overview"]["delivered_cost"], expected_cost)

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
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
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
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        acc_data = response.data
        self.assertIsNotNone(acc_data)
        self.assertIn("cost", acc_data)
        self.assertIn("plan_cpm", acc_data)
        self.assertIn("plan_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)
        self.assertIn("average_cpv", acc_data)
        self.assertIn("cost", acc_data["overview"])
        self.assertIn("average_cpm", acc_data["overview"])
        self.assertIn("average_cpv", acc_data["overview"])
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        acc_data = response.data
        self.assertIsNotNone(acc_data)
        self.assertNotIn("cost", acc_data)
        self.assertNotIn("plan_cpm", acc_data)
        self.assertNotIn("plan_cpv", acc_data)
        self.assertNotIn("average_cpm", acc_data)
        self.assertNotIn("average_cpv", acc_data)
        self.assertNotIn("cost", acc_data["overview"])
        self.assertNotIn("average_cpm", acc_data["overview"])
        self.assertNotIn("average_cpv", acc_data["overview"])

    def test_brand(self):
        self.user.is_staff = True
        self.user.save()
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="2", name="")
        account_creation = AccountCreation.objects.create(
            name="Test", owner=self.user, account=managed_account)
        managed_account.managers.add(chf_account)
        test_brand = "Test Brand"
        opportunity = Opportunity.objects.create(brand=test_brand)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["brand"], test_brand)

    def test_success_get_chf_account(self):
        user = self.user
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        managed_account.managers.add(chf_account)
        account_creation = AccountCreation.objects.create(
            name="Test", owner=user, account=managed_account)
        user.is_staff = True
        user.save()
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [managed_account.id]
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_fields = (
            "delivered_cost", "plan_cost", "delivered_impressions",
            "plan_impressions", "delivered_video_views", "plan_video_views")
        self.assertTrue(
            all([field in response.data["overview"]
                 for field in expected_fields]))

    def test_agency(self):
        self.user.is_staff = True
        self.user.save()
        agency = Contact.objects.create(first_name="first", last_name="last")
        opportunity = Opportunity.objects.create(agency=agency)
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        managed_account.managers.add(chf_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user, account=managed_account)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["agency"], agency.name)

    def test_cost_method(self):
        self.user.is_staff = True
        self.user.save()
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
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user, account=managed_account)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_campaigns_filter_affect_performance_data(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
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
            response = self._request(account_creation.id,
                                     campaigns=[campaign_one_id])
        self.assertEqual(response.status_code, HTTP_200_OK)
        overview_section = response.data["overview"]
        self.assertEqual(overview_section["plan_cost"], expected_plan_cost)
        self.assertEqual(overview_section["plan_impressions"], expected_plan_impressions)
        self.assertEqual(overview_section["delivered_cost"], expected_delivered_cost)
        self.assertEqual(overview_section["delivered_impressions"], expected_delivered_impressions)

    def test_ad_groups_filter_affect_performance_data(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
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
            response = self._request(account_creation.id,
                                     ad_groups=[ad_group_one_id])
        self.assertEqual(response.status_code, HTTP_200_OK)
        overview_section = response.data["overview"]
        self.assertEqual(overview_section["plan_cost"], expected_plan_cost)
        self.assertEqual(overview_section["plan_impressions"], expected_plan_impressions)
        self.assertEqual(overview_section["delivered_cost"], expected_delivered_cost)
        self.assertEqual(overview_section["delivered_impressions"], expected_delivered_impressions)

    def test_overview_reflects_to_date_range(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
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
            response = self._request(account_creation.id,
                                     start_date=str(date_1),
                                     end_date=str(date_1))
        self.assertEqual(response.status_code, HTTP_200_OK)
        overview_section = response.data["overview"]
        self.assertEqual(overview_section["cost"], cost_1)
        self.assertEqual(overview_section["impressions"], impressions_1)
        self.assertEqual(overview_section["video_views"], views_1)

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
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpm,
                                account=account)
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpv,
                                account=account)

        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        with self.subTest("CPM"):
            self.assertIsNone(response.data["plan_cpm"])
        with self.subTest("CPV"):
            self.assertIsNone(response.data["plan_cpv"])

    def test_dashboard_planned_cpv_and_cpm_are_none(self):
        self.user.add_custom_user_permission("view_dashboard")
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
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
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
                response = self._request(account_creation.id)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(response.data["id"], account_creation.id)
                actual_value = response.data.get(key)
                if cost_hidden:
                    self.assertIsNone(actual_value)
                else:
                    self.assertIsNotNone(actual_value)
                    self.assertAlmostEqual(actual_value, expected_value)

    def test_average_cpm_and_cpv_not_reflect_to_user_settings(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""),
            user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user, is_approved=True)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)
        # hide
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        self.user.add_custom_user_permission("view_dashboard")
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
