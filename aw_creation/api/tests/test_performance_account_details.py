import json
from datetime import datetime, timedelta, date
from unittest.mock import patch

import pytz
from django.conf import settings
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, AdCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, IMPRESSIONS, \
    TOTAL_DEMO_AD_GROUPS_COUNT
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    GeoTarget, CityStatistic, AWConnection, AWConnectionToUserRelation, \
    SalesForceGoalType, OpPlacement, Opportunity, Contact, Flight
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase, int_iterator
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountDetailsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.ACCOUNT_DETAILS,
            args=(account_creation_id,))

    account_list_header_fields = {
        "id", "name", "end", "account", "start", "status", "weekly_chart",
        "thumbnail", "is_changed",
        "clicks", "cost", "impressions", "video_views", "video_view_rate",
        "is_managed",
        "ad_count", "channel_count", "video_count", "interest_count",
        "topic_count", "keyword_count",
        "is_disapproved", "from_aw", "updated_at",
        "cost_method", "agency", "brand", "average_cpm", "average_cpv",
        "ctr", "ctr_v", "plan_cpm", "plan_cpv"
    }
    overview_keys = {
        "age", "gender", "device", "location",
        "clicks", "cost", "impressions", "video_views",
        "ctr", "ctr_v", "average_cpm", "average_cpv",
        "all_conversions", "conversions", "view_through",
        "video_view_rate",
        "video100rate", "video25rate", "video50rate",
        "video75rate", "video_views_this_week",
        "video_view_rate_top", "impressions_this_week",
        "video_views_last_week", "cost_this_week",
        "video_view_rate_bottom", "clicks_this_week",
        "ctr_v_top", "cost_last_week", "average_cpv_top",
        "ctr_v_bottom", "ctr_bottom", "clicks_last_week",
        "average_cpv_bottom", "ctr_top", "impressions_last_week",

        "plan_video_views", "delivered_impressions", "plan_impressions",
        "delivered_cost", "delivered_video_views", "plan_cost",
        "video_clicks"
    }

    detail_keys = {
        "creative",
        "age", "gender", "device",
        "all_conversions", "conversions", "view_through", "average_position",
        "video100rate", "video25rate", "video50rate", "video75rate",
        "delivery_trend", "ad_network"
    }

    def setUp(self):
        self.user = self.create_test_user()
        self.user.aw_settings[UserSettingsKey.SHOW_CONVERSIONS] = True
        self.user.save()

    def test_success_get(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.user)
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(
            name="", is_managed=False, owner=self.user,
            account=account, is_approved=True)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1)
        campaign = Campaign.objects.create(
            id=1, name="", account=account, **stats)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        date = datetime.now().date() - timedelta(days=1)
        ad_network = "ad_network"
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=date, average_position=1,
            ad_network=ad_network, **stats)
        target, _ = GeoTarget.objects.get_or_create(
            id=1, defaults=dict(name=""))
        CityStatistic.objects.create(
            ad_group=ad_group, date=date, city=target, **stats)
        url = self._get_url(account_creation.id)
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(
                    dict(start_date=str(date - timedelta(days=1)),
                         end_date=str(date))),
                content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"})
        self.assertEqual(set(data["details"].keys()), self.detail_keys)
        self.assertEqual(data['details']['video25rate'], 100)
        self.assertEqual(data['details']['video50rate'], 75)
        self.assertEqual(data['details']['video75rate'], 50)
        self.assertEqual(data['details']['video100rate'], 25)
        self.assertEqual(data['details']['ad_network'], ad_network)
        self.assertEqual(set(data["overview"].keys()), self.overview_keys)

    def test_success_get_chf_account(self):
        user = self.create_test_user()
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        managed_account.managers.add(chf_account)
        account_creation = AccountCreation.objects.create(
            name="Test", owner=self.user, account=managed_account)
        url = self._get_url(account_creation.id)
        user.is_staff = True
        user.save()
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [managed_account.id]}
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, data=json.dumps({"is_chf": 1}),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_fields = (
            "delivered_cost", "plan_cost", "delivered_impressions",
            "plan_impressions", "delivered_video_views", "plan_video_views")
        self.assertTrue(
            all([field in response.data["overview"]
                 for field in expected_fields]))

    def test_success_get_no_account(self):
        # add a connection not to show demo data
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.user)
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user, sync_at=timezone.now())
        account = Account.objects.create(id=1, name="")
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        AdGroupStatistic.objects.create(
            date=datetime.now(), ad_group=ad_group,
            average_position=1, impressions=100)
        url = self._get_url(account_creation.id)
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"})
        self.assertEqual(set(data["details"].keys()), self.detail_keys)
        self.assertEqual(set(data["overview"].keys()), self.overview_keys)
        self.assertIs(data['impressions'], None)
        self.assertIs(data['overview']['impressions'], None)

    def test_success_get_filter_dates_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        today = datetime.now().date()
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(
                    dict(start_date=str(today - timedelta(days=2)),
                         end_date=str(today - timedelta(days=1)))),
                content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"})
        self.assertEqual(set(data["details"].keys()), self.detail_keys)
        self.assertEqual(set(data["overview"].keys()), self.overview_keys)
        self.assertEqual(
            data["details"]['delivery_trend'][0]['label'], "Impressions")
        self.assertEqual(
            data["details"]['delivery_trend'][1]['label'], "Views")
        self.assertEqual(data['overview']['impressions'], IMPRESSIONS / 10)

    def test_success_get_filter_ad_groups_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        ad_groups = ["demo11", "demo22"]
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(ad_groups=ad_groups)),
                content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.account_list_header_fields | {"details", "overview"})
        self.assertEqual(set(data["details"].keys()), self.detail_keys)
        self.assertEqual(set(data["overview"].keys()), self.overview_keys)
        self.assertEqual(
            data['overview']['impressions'],
            IMPRESSIONS / TOTAL_DEMO_AD_GROUPS_COUNT * len(ad_groups))

    def test_success_get_demo_data(self):
        account_creation = AccountCreation.objects.create(
            name="Name 123", owner=self.user)
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation)
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation)
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
            video_thumbnail="https://f.i/123.jpeg")
        url = self._get_url(account_creation.id)
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data['id'], account_creation.id)
        self.assertEqual(data['name'], account_creation.name)
        self.assertEqual(data['status'], account_creation.status)
        self.assertEqual(data['thumbnail'], ad_creation.video_thumbnail)
        self.assertIsNotNone(data['impressions'])
        self.assertIsNotNone(data['overview']['impressions'])

    def test_updated_at(self):
        test_time = datetime(2017, 1, 1, tzinfo=pytz.utc)
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.user)
        account = Account.objects.create(update_time=test_time)
        account_creation = AccountCreation.objects.create(
            name="Name 123", account=account,
            is_approved=True, owner=self.user)
        url = self._get_url(account_creation.id)
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], test_time)

    def test_created_at_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], None)

    def test_details_for_chf_acc(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        response = self.client.post(
            url, json.dumps(dict(is_chf=1)), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], None)

    def test_average_cpm_and_cpv(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
        impressions, views, cost = 1, 2, 3
        Campaign.objects.create(
            account=account, impressions=impressions,
            video_views=views, cost=cost)
        average_cpm = cost / impressions * 1000
        average_cpv = cost / views
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertAlmostEqual(response.data["average_cpm"], average_cpm)
        self.assertAlmostEqual(response.data["average_cpv"], average_cpv)

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
        url = self._get_url(account_creation.id)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True}
        self.user.add_custom_user_permission("view_dashboard")
        with self.patch_user_settings(**user_settings), \
             self.subTest("hide"):
            response = self.client.post(url, dict(is_chf=1))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["id"], account_creation.id)
            self.assertNotIn("average_cpm", response.data)
            self.assertNotIn("average_cpv", response.data)
            self.assertNotIn("plan_cpm", response.data)
            self.assertNotIn("plan_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False}
        with self.patch_user_settings(**user_settings), \
             self.subTest("show"):
            response = self.client.post(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["id"], account_creation.id)
            self.assertIn("average_cpm", response.data)
            self.assertIn("average_cpv", response.data)
            self.assertIn("plan_cpm", response.data)
            self.assertIn("plan_cpv", response.data)

    def test_plan_cpm_and_cpv(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user, is_approved=True)
        account_creation.refresh_from_db()
        opportunity = Opportunity.objects.create()
        placement_cpm = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_units=123, total_cost=345)
        placement_cpv = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_units=234, total_cost=123)
        expected_cpm = placement_cpm.total_cost / \
                       placement_cpm.ordered_units * 1000
        expected_cpv = placement_cpv.total_cost / placement_cpv.ordered_units
        Campaign.objects.create(
            id=1, account=account, salesforce_placement=placement_cpv)
        Campaign.objects.create(
            id=2, account=account, salesforce_placement=placement_cpm)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertAlmostEqual(response.data["plan_cpm"], expected_cpm)
        self.assertAlmostEqual(response.data["plan_cpv"], expected_cpv)

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
        url = self._get_url(account_creation.id)
        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True}
        self.user.add_custom_user_permission("view_dashboard")
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict(is_chf=1))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertNotIn("plan_cpm", response.data)
        self.assertNotIn("plan_cpv", response.data)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertIn("plan_cpm", response.data)
        self.assertIn("plan_cpv", response.data)

    def test_ctr_and_ctr_v(self):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
        impressions, views, clicks = 1, 2, 3
        Campaign.objects.create(
            account=account, impressions=impressions,
            video_views=views, clicks=clicks)
        ctr = clicks / impressions * 100
        ctr_v = clicks / views * 100
        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertAlmostEqual(response.data["ctr"], ctr)
        self.assertAlmostEqual(response.data["ctr_v"], ctr_v)

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
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, json.dumps(dict(is_chf=1)),
                                        content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["cost"], expected_cost)
        self.assertAlmostEqual(
            response.data["overview"]["delivered_cost"], expected_cost)

    def test_aw_cost_sf_linked_only(self):
        self.user.is_staff = True
        self.user.save()
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user, is_approved=True)
        account_creation.refresh_from_db()
        costs = (123, 234)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign_one = Campaign.objects.create(
            id=1, account=account,
            salesforce_placement=placement, cost=costs[0])
        campaign_two = Campaign.objects.create(
            id=2, account=account, cost=costs[1])
        AdGroup.objects.create(id=1, campaign=campaign_one, cost=costs[0])
        AdGroup.objects.create(id=2, campaign=campaign_two, cost=costs[1])
        expected_cost = sum(costs)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, json.dumps(dict(is_chf=1)),
                                        content_type="application/json")
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
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
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
        url = self._get_url(account_creation.id)
        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, json.dumps(dict(is_chf=1)),
                                        content_type="application/json")
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
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
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
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["brand"], test_brand)

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
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
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
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_delivered_impressions(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
        opportunity = Opportunity.objects.create(id=1)
        placement_cpm = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM)
        placement_cpv = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV)
        impressions_cpm, impressions_cpv = 2345, 2345
        campaign_one = Campaign.objects.create(
            id=1, salesforce_placement=placement_cpm, account=account,
            impressions=impressions_cpm)
        campaign_two = Campaign.objects.create(
            id=2, salesforce_placement=placement_cpv, account=account,
            impressions=impressions_cpv)
        AdGroup.objects.create(
            id=1, campaign=campaign_one, impressions=impressions_cpm)
        AdGroup.objects.create(
            id=2, campaign=campaign_two, impressions=impressions_cpv)
        self.assertGreater(impressions_cpv, 0)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1)),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        delivered_impressions = response.data["overview"][
            "delivered_impressions"]
        self.assertEqual(delivered_impressions, impressions_cpm)

    def test_campaigns_filter_affect_performance_data(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
        opportunity = Opportunity.objects.create(id=1)
        expected_plan_cost = 3
        expected_plan_impressions = 3
        expected_plan_video_views = 0
        placement_one = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            total_cost=expected_plan_cost,
            ordered_units=expected_plan_impressions)
        placement_two = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
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
        AdGroup.objects.create(
            id=ad_group_one_id, campaign=campaign_one,
            cost=expected_delivered_cost,
            video_views=expected_delivered_video_views,
            impressions=expected_delivered_impressions)
        AdGroup.objects.create(
            id=ad_group_two_id, campaign=campaign_two, cost=1,
            video_views=1, impressions=1)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1, campaigns=[campaign_one_id])),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        overview_section = response.data["overview"]
        self.assertEqual(overview_section["plan_cost"], expected_plan_cost)
        self.assertEqual(
            overview_section["plan_impressions"], expected_plan_impressions)
        self.assertEqual(
            overview_section["plan_video_views"], expected_plan_video_views)
        self.assertEqual(
            overview_section["delivered_cost"], expected_delivered_cost)
        self.assertEqual(
            overview_section["delivered_impressions"],
            expected_delivered_impressions)
        self.assertEqual(
            overview_section["delivered_video_views"],
            expected_delivered_video_views)

    def test_ad_groups_filter_affect_performance_data(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        account = Account.objects.create(id=1)
        account_creation = AccountCreation.objects.create(
            id=1, owner=user, account=account)
        opportunity = Opportunity.objects.create(id=1)
        expected_plan_cost = 3
        expected_plan_impressions = 3
        expected_plan_video_views = 0
        placement_one = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            total_cost=expected_plan_cost,
            ordered_units=expected_plan_impressions)
        placement_two = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
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
        AdGroup.objects.create(
            id=ad_group_one_id, campaign=campaign_one,
            cost=expected_delivered_cost,
            video_views=expected_delivered_video_views,
            impressions=expected_delivered_impressions)
        AdGroup.objects.create(
            id=ad_group_two_id, campaign=campaign_two, cost=1,
            video_views=1, impressions=1)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url, json.dumps(dict(is_chf=1, ad_groups=[ad_group_one_id])),
                content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        overview_section = response.data["overview"]
        self.assertEqual(overview_section["plan_cost"], expected_plan_cost)
        self.assertEqual(
            overview_section["plan_impressions"], expected_plan_impressions)
        self.assertEqual(
            overview_section["plan_video_views"], expected_plan_video_views)
        self.assertEqual(
            overview_section["delivered_cost"], expected_delivered_cost)
        self.assertEqual(
            overview_section["delivered_impressions"],
            expected_delivered_impressions)
        self.assertEqual(
            overview_section["delivered_video_views"],
            expected_delivered_video_views)

    def test_cpv_and_cpm_sf_data(self):
        opportunity = Opportunity.objects.create()
        total_cost_cpv = (34, 45)
        total_cost_cpm = (56, 67)
        ordered_units_cpv = (123, 234)
        ordered_units_cpm = (1234, 2345)
        placement_cpv_1 = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            total_cost=total_cost_cpv[0], ordered_units=ordered_units_cpv[0],
            ordered_rate=1.2)
        placement_cpv_2 = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            total_cost=total_cost_cpv[1], ordered_units=ordered_units_cpv[1],
            ordered_rate=1.3)
        placement_cpm_1 = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            total_cost=total_cost_cpm[0], ordered_units=ordered_units_cpm[0],
            ordered_rate=1.4)
        placement_cpm_2 = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            total_cost=total_cost_cpm[1], ordered_units=ordered_units_cpm[1],
            ordered_rate=1.5)
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=self.request_user)
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, account=account, owner=self.request_user,
            is_approved=True)
        account_creation.refresh_from_db()
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpm_1,
                                account=account)
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpm_2,
                                account=account)
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpv_1,
                                account=account)
        Campaign.objects.create(id=next(int_iterator),
                                salesforce_placement=placement_cpv_2,
                                account=account)

        plan_cpm = sum(total_cost_cpm) / sum(ordered_units_cpm) * 1000
        plan_cpv = sum(total_cost_cpv) / sum(ordered_units_cpv)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        with self.subTest("CPM"):
            self.assertIsNotNone(response.data["plan_cpm"])
            self.assertAlmostEqual(response.data["plan_cpm"], plan_cpm)
        with self.subTest("CPV"):
            self.assertIsNotNone(response.data["plan_cpv"])
            self.assertAlmostEqual(response.data["plan_cpv"], plan_cpv)

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
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False}
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        with self.subTest("CPM"):
            self.assertIsNone(response.data["plan_cpm"])
        with self.subTest("CPV"):
            self.assertIsNone(response.data["plan_cpv"])
