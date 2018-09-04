from datetime import datetime
from datetime import timedelta
from itertools import product
from unittest.mock import patch
from urllib.parse import urlencode

from django.conf import settings
from django.test import override_settings
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import DEMO_AGENCY
from aw_reporting.demo.models import DEMO_BRAND
from aw_reporting.demo.models import DEMO_COST_METHOD
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import Contact
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import goal_type_str
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import generic_test
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class DashboardAccountCreationListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "account",
        "ad_count",
        "agency",
        "average_cpm",
        "average_cpv",
        "brand",
        "channel_count",
        "clicks",
        "cost",
        "cost_method",
        "ctr",
        "ctr_v",
        "ctr_v",
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
        "start",
        "thumbnail",
        "topic_count",
        "updated_at",
        "video_count",
        "video_view_rate",
        "video_views",
        "weekly_chart",
    }

    url = reverse(Name.Dashboard.ACCOUNT_LIST, [RootNamespace.AW_CREATION, Namespace.DASHBOARD])

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_dashboard")
        self.mcc_account = Account.objects.create(can_manage_clients=True)
        aw_connection = AWConnection.objects.create(refresh_token="token")
        AWAccountPermission.objects.create(aw_connection=aw_connection, account=self.mcc_account)
        AWConnectionToUserRelation.objects.create(connection=aw_connection, user=self.user)

    def __set_non_admin_user_with_account(self, account_id):
        user = self.user
        user.is_staff = False
        user.is_superuser = False
        user.update_access([{"name": "Tools", "value": True}])
        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account_id]
        user.save()

    def test_success_get(self):
        account = Account.objects.create(id="123", name="")
        account.managers.add(self.mcc_account)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=date,
                                              ad_group=ad_group,
                                              impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=date,
                                              ad_group=ad_group,
                                              impressions=12)

        ac_creation = account.account_creation
        camp_creation = CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            goal_units=100, max_rate="0.07",
            start=datetime.now() - timedelta(days=10),
            end=datetime.now() + timedelta(days=10),
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
        )
        AdCreation.objects.create(name="", ad_group_creation=ad_group_creation,
                                  video_thumbnail="http://some.url.com")
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation, campaign=None,
        )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             override_settings(CHANNEL_FACTORY_ACCOUNT_ID=self.mcc_account.id), \
             self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                "max_page",
                "items_count",
                "items",
                "current_page",
            }
        )
        self.assertEqual(response.data["items_count"], 2)
        self.assertEqual(len(response.data["items"]), 2)
        item = response.data["items"][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )

    def test_properties_demo(self):
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(set(response.data["items"][0].keys()), self.details_keys)

    def test_get_chf_account_creation_list_queryset(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        expected_account_id = "1"
        managed_account = Account.objects.create(
            id=expected_account_id, name="")
        managed_account.managers.add(chf_account)
        Account.objects.create(id="2", name="")
        Account.objects.create(id="3", name="")
        Account.objects.create(id="4", name="")
        self.__set_non_admin_user_with_account(managed_account.id)
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts_ids = {a["account"] for a in response.data["items"]}
        self.assertEqual(accounts_ids, {"demo", expected_account_id})

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
        self.__set_non_admin_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[managed_account.account_creation.id]["brand"], test_brand)

    def test_agency(self):
        agency = Contact.objects.create(first_name="first", last_name="last")
        opportunity = Opportunity.objects.create(agency=agency)
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        managed_account.managers.add(chf_account)
        self.__set_non_admin_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[managed_account.account_creation.id]["agency"], agency.name)

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
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        self.__set_non_admin_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(
            set(accounts[account_creation.id]["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_cost_client_cost_dashboard(self):
        manager = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        account.save()
        opportunity = Opportunity.objects.create()
        placement_cpm = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=2.)
        placement_cpv = OpPlacement.objects.create(
            id=2, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=2.)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=3, opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE)
        placement_hard_cost = OpPlacement.objects.create(
            id=4, opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=523)
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
                impressions=632, cost=241)
        )

        client_cost = sum(
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
                end=c.end_date
            )
                for c in campaigns]
        )
        aw_cost = sum([c.cost for c in campaigns])
        self.assertNotEqual(client_cost, aw_cost)

        test_cases = (
            (True, aw_cost),
            (False, client_cost),
        )
        for aw_rates, expected_cost in test_cases:
            user_settings = {
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rates,
                UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
            }
            with self.subTest(**user_settings), \
                 self.patch_user_settings(**user_settings):
                response = self.client.get(self.url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                accs = dict((acc["id"], acc) for acc in response.data["items"])
                acc_data = accs.get(account.account_creation.id)
                self.assertIsNotNone(acc_data)
                self.assertAlmostEqual(acc_data["cost"], expected_cost)

    def test_demo_brand(self):
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["brand"], DEMO_BRAND)

    def test_demo_cost_type(self):
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["cost_method"], DEMO_COST_METHOD)

    def test_demo_agency(self):
        # hide
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["agency"], DEMO_AGENCY)

    def test_list_only_chf_accounts(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        another_mcc_account = Account.objects.create(id=next(int_iterator), can_manage_clients=True)
        visible_account = Account.objects.create(id=next(int_iterator))
        visible_account.managers.add(chf_mcc_account)
        hidden_account = Account.objects.create(id=next(int_iterator))
        hidden_account.managers.add(another_mcc_account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = response.data["items"]
        self.assertEqual(len(accounts), 2)
        self.assertEqual(accounts[1]["id"], visible_account.account_creation.id)

    def test_no_demo_data(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(chf_mcc_account)
        account.save()
        Campaign.objects.create(id=next(int_iterator), account=account)

        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        item = response.data["items"][0]
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

    def test_no_status_filters(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(chf_mcc_account)
        account.save()
        account.account_creation.is_paused = False
        account.account_creation.save()
        Campaign.objects.create(id=next(int_iterator), account=account)

        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        url = "?".join([
            self.url,
            urlencode(dict(status="Paused")),
        ])

        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)

    @generic_test([
        (
                "Goal type: {}, Dynamic placement: {}".format(goal_type_str(goal_type_id), dynamic_placement),
                (goal_type_id, dynamic_placement),
                dict()
        )
        for goal_type_id, dynamic_placement in product(
            (SalesForceGoalType.CPM, SalesForceGoalType.CPV),
            (DynamicPlacementType.BUDGET, DynamicPlacementType.RATE_AND_TECH_FEE),
        )
    ])
    def test_dynamic_placements_rates(self, goal_type_id, dynamic_placement):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity,
                                               goal_type_id=goal_type_id,
                                               dynamic_placement=dynamic_placement,
                                               tech_fee=1)
        Flight.objects.create(placement=placement,
                              ordered_units=1, total_cost=1, cost=1, delivered=1, ordered_cost=1)
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(chf_mcc_account)
        account.save()

        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
        }

        Campaign.objects.create(account=account, salesforce_placement=placement,
                                cost=1, impressions=2000, video_views=30)

        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        item = response.data["items"][0]

        self.assertAlmostEqual(item["average_cpm"], item["cost"] / item["impressions"] * 1000)
        self.assertAlmostEqual(item["average_cpv"], item["cost"] / item["video_views"])
