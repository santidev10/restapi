import json
from datetime import datetime
from datetime import timedelta
from itertools import product
from unittest.mock import patch

import pytz
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import IMPRESSIONS
from aw_reporting.demo.models import TOTAL_DEMO_AD_GROUPS_COUNT
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CityStatistic
from aw_reporting.models import Flight
from aw_reporting.models import GeoTarget
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AnalyticsAccountDetailsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            RootNamespace.AW_CREATION + ":" + Namespace.ANALYTICS + ":" + Name.Analytics.ACCOUNT_DETAILS,
            args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(dict(is_chf=0, **kwargs)), content_type="application/json")

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
        "video_clicks",

        "has_statistics",
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
        user_settings = {UserSettingsKey.SHOW_CONVERSIONS: True}
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id,
                                     start_date=str(date - timedelta(days=1)),
                                     end_date=str(date))
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
        user_settings = {UserSettingsKey.SHOW_CONVERSIONS: True}
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
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
        today = datetime.now().date()
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(DEMO_ACCOUNT_ID,
                                     start_date=str(today - timedelta(days=2)),
                                     end_date=str(today - timedelta(days=1)))
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
        ad_groups = ["demo11", "demo22"]
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(DEMO_ACCOUNT_ID, ad_groups=ad_groups)
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
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(account_creation.id)
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
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertIn("updated_at", data)
        self.assertEqual(data["updated_at"], test_time)

    def test_created_at_demo(self):
        response = self._request(DEMO_ACCOUNT_ID)
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
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
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
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
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
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
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
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertAlmostEqual(response.data["plan_cpm"], expected_cpm)
        self.assertAlmostEqual(response.data["plan_cpv"], expected_cpv)

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
        response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
        self.assertAlmostEqual(response.data["ctr"], ctr)
        self.assertAlmostEqual(response.data["ctr_v"], ctr_v)

    def test_analytics_planned_cpv_and_cpm_are_none(self):
        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(
            id=1,
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_units=1000, ordered_rate=1.2)
        placement_cpm = OpPlacement.objects.create(
            id=2,
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_units=1000, ordered_rate=1.3)
        total_cost_cpv = (34, 45)
        total_cost_cpm = (56, 67)
        ordered_units_cpv = (123, 234)
        ordered_units_cpm = (1234, 2345)
        Flight.objects.create(id=1, placement=placement_cpm,
                              total_cost=total_cost_cpm[0],
                              ordered_units=ordered_units_cpm[0])
        Flight.objects.create(id=2, placement=placement_cpm,
                              total_cost=total_cost_cpm[1],
                              ordered_units=ordered_units_cpm[1])
        Flight.objects.create(id=3, placement=placement_cpv,
                              total_cost=total_cost_cpv[0],
                              ordered_units=ordered_units_cpv[0])
        Flight.objects.create(id=4, placement=placement_cpv,
                              total_cost=total_cost_cpv[1],
                              ordered_units=ordered_units_cpv[1])
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

        plan_cpm = sum(total_cost_cpm) / sum(ordered_units_cpm) * 1000
        plan_cpv = sum(total_cost_cpv) / sum(ordered_units_cpv)
        self.assertIsNotNone(plan_cpm)
        self.assertIsNotNone(plan_cpv)
        costs_hidden_cases = (True, False)
        ad_words_rates_cases = (True, False)
        keys = (
            ("CPM", "plan_cpm"),
            ("CPv", "plan_cpv"),
        )
        test_cases = product(costs_hidden_cases, ad_words_rates_cases, keys)
        for cost_hidden, aw_rate, msg_key in test_cases:
            msg, key = msg_key
            user_settings = {
                UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: cost_hidden,
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rate
            }

            with self.subTest(msg, **user_settings), \
                 self.patch_user_settings(**user_settings):
                response = self._request(account_creation.id)
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(response.data["id"], account_creation.id)
                self.assertIsNone(response.data[key])
