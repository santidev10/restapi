import json
from datetime import date
from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.db.models import Sum
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountDetailTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_DETAIL,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(kwargs), content_type="application/json")

    account_list_header_fields = {
        "account",
        "average_cpm",
        "average_cpv",
        "clicks",
        "cost",
        "ctr",
        "ctr_v",
        "details",
        "end",
        "from_aw",
        "id",
        "impressions",
        "is_changed",
        "is_disapproved",
        "is_editable",
        "is_managed",
        "name",
        "plan_cpm",
        "plan_cpv",
        "start",
        "statistic_max_date",
        "statistic_min_date",
        "status",
        "thumbnail",
        "updated_at",
        "video_view_rate",
        "video_views",
        "weekly_chart",
        "clicks_app_store",
        "clicks_end_cap",
        "clicks_call_to_action_overlay",
        "clicks_cards",
        "clicks_website",
    }
    detail_keys = {
        "ad_network",
        "age",
        "all_conversions",
        "average_position",
        "conversions",
        "creative",
        "delivery_trend",
        "device",
        "gender",
        "video100rate",
        "video25rate",
        "video50rate",
        "video75rate",
        "view_through",
    }

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_non_visible_account(self):
        user = self.create_admin_user()
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_get(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", is_managed=False, owner=user,
            account=account, is_approved=True)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1)
        campaign = Campaign.objects.create(
            id=1, name="", account=account, **stats)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = datetime.now().date() - timedelta(days=1)
        ad_network = "ad_network"
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=yesterday, average_position=1,
            ad_network=ad_network, **stats)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_visible_account(self):
        self.create_admin_user()
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_aw_cost(self):
        self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
            StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST: True,
        })
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
        response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)

    def test_cost_client_cost(self):
        user = self.create_admin_user()
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
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)

    def test_cost_method(self):
        user = self.create_admin_user()
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
            response = self.client.get(self._get_url(account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_dynamic_placement_budget_rates_are_empty(self):
        user = self.create_admin_user()
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
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation_id)
        with self.subTest("CPM"):
            self.assertIsNone(response.data["plan_cpm"])
        with self.subTest("CPV"):
            self.assertIsNone(response.data["plan_cpv"])

    def test_rates_on_multiple_campaigns(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/VIQ-278
        Summary: Dashboard > Incorrect cpv/ cpm on Dashboard for Dynamic placement if several placements with the same type are present
        Root cause: stats aggregates multiple times on several Campaign-Placement relations
        """
        self.create_admin_user()
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
            response = self.client.get(self._get_url(account.account_creation.id))

        self.assertEqual(response.status_code, HTTP_200_OK)
        item = response.data
        with self.subTest("CPM"):
            self.assertAlmostEqual(item["average_cpm"], expected_cpm)
        with self.subTest("CPV"):
            self.assertAlmostEqual(item["average_cpv"], expected_cpv)

    def test_min_max_based_on_statistic(self):
        self.create_admin_user()
        account = Account.objects.create(
            id=next(int_iterator),
        )
        campaign = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
        )
        dates = [date(2019, 1, 1) + timedelta(days=i) for i in range(30)]
        for dt in dates:
            CampaignStatistic.objects.create(
                campaign=campaign,
                cost=1,
                date=dt,
            )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["statistic_min_date"], dates[0])
        self.assertEqual(data["statistic_max_date"], dates[-1])

    def test_no_overcalculate_statistic(self):
        self.create_admin_user()
        account = Account.objects.create(
            id=next(int_iterator),
        )
        campaign = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
            impressions=1,
        )
        dates = [date(2019, 1, 1) + timedelta(days=i) for i in range(5)]
        for dt in dates:
            CampaignStatistic.objects.create(
                campaign=campaign,
                cost=1,
                date=dt,
            )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["impressions"], campaign.impressions)

    def test_video_views_impressions_ad_group_type(self):
        self.create_admin_user()
        account = Account.objects.create()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign_1 = Campaign.objects.create(
            id=1, account=account, impressions=5213, video_views=4111,
            salesforce_placement=placement)
        campaign_2 = Campaign.objects.create(
            id=2, account=account,
            salesforce_placement=placement)
        campaign_3 = Campaign.objects.create(
            id=3, account=account, impressions=7311, video_views=2141,
            salesforce_placement=placement)
        AdGroup.objects.create(
            id=1, campaign=campaign_1, type="In-stream")
        AdGroup.objects.create(
            id=2, campaign=campaign_2, type="Bumper")
        AdGroup.objects.create(
            id=3, campaign=campaign_3, type="In-stream")

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        impressions = campaign_1.impressions + campaign_3.impressions
        video_views = campaign_1.video_views + campaign_3.video_views
        self.assertEqual(response.data["impressions"], impressions)
        self.assertEqual(response.data["video_views"], video_views)
        self.assertAlmostEqual(response.data["video_view_rate"], (video_views / impressions) * 100)
