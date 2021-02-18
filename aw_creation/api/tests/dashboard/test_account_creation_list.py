from datetime import date
from datetime import datetime
from datetime import timedelta
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
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.data import DEMO_BRAND
from aw_reporting.demo.data import DEMO_COST_METHOD
from aw_reporting.demo.data import DEMO_SF_ACCOUNT
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SFAccount
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.reverse import reverse
from utils.unittests.str_iterator import str_iterator


class DashboardAccountCreationListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "account",
        "all_conversions",
        "average_cpm",
        "average_cpv",
        "brand",
        "clicks",
        "cost",
        "cost_method",
        "ctr",
        "ctr_v",
        "ctr_v",
        "currency_code",
        "end",
        "id",
        "impressions",
        "is_changed",
        "is_disapproved",
        "name",
        "plan_cpm",
        "plan_cpv",
        "sf_account",
        "start",
        "statistic_min_date",
        "statistic_max_date",
        "thumbnail",
        "updated_at",
        "video_view_rate",
        "video_views",
        "weekly_chart",
        "status",
    }

    url = reverse(Name.Dashboard.ACCOUNT_LIST, [RootNamespace.AW_CREATION, Namespace.DASHBOARD])

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
        })
        self.mcc_account = Account.objects.create(can_manage_clients=True)
        aw_connection = AWConnection.objects.create(refresh_token="token")
        AWAccountPermission.objects.create(aw_connection=aw_connection, account=self.mcc_account)
        AWConnectionToUserRelation.objects.create(connection=aw_connection, user=self.user)

    def __set_non_admin_user_with_account(self, account_id):
        user = self.user
        user.perms.update({
            StaticPermissions.MANAGED_SERVICE: True,
        })
        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account_id]
        user.save()

    def test_success_get(self):
        self.user.perms.update({
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
            StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS: True,
        })
        self.user.save()
        account = Account.objects.create(name="")
        account.managers.add(self.mcc_account)
        campaign = Campaign.objects.create(name="", account=account)
        ad_group = AdGroup.objects.create(name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        action_date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=action_date,
                                              ad_group=ad_group,
                                              impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=action_date,
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
        with override_settings(MCC_ACCOUNT_IDS=[self.mcc_account.id]):
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
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(len(response.data["items"]), 1)
        item = response.data["items"][0]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )

    def test_properties_demo(self):
        recreate_test_demo_data()
        self.user.perms.update({
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_DEMO_ACCOUNT: True,
            StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS: True,
        })
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(set(response.data["items"][0].keys()),
                         self.details_keys)

    def test_media_buying(self):
        """ Test that media_buying permissions grants access as both features use this endpoint """
        self.user.perms.update({
            StaticPermissions.MANAGED_SERVICE: False,
            StaticPermissions.MEDIA_BUYING: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
            StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS: True,
        })
        self.user.save()
        Account.objects.create(name="")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_get_chf_account_creation_list_queryset(self):
        recreate_test_demo_data()
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        expected_account_id = 1
        managed_account = Account.objects.create(
            id=expected_account_id, name="")
        managed_account.managers.add(chf_account)
        Account.objects.create(name="")
        Account.objects.create(name="")
        Account.objects.create(name="")
        self.__set_non_admin_user_with_account(managed_account.id)
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_DEMO_ACCOUNT] = True
        self.user.save()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts_ids = {a["account"] for a in response.data["items"]}
        self.assertEqual(accounts_ids, {DEMO_ACCOUNT_ID, expected_account_id})

    def test_brand(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(name="")
        managed_account.managers.add(chf_account)
        test_brand = "Test Brand"
        opportunity = Opportunity.objects.create(brand=test_brand)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        self.__set_non_admin_user_with_account(managed_account.id)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[managed_account.account_creation.id]["brand"], test_brand)

    def test_sf_account(self):
        sf_account = SFAccount.objects.create(name="test name")
        opportunity = Opportunity.objects.create(account=sf_account)
        placement = OpPlacement.objects.create(id=next(str_iterator), opportunity=opportunity)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(name="")
        Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        managed_account.managers.add(chf_account)
        self.__set_non_admin_user_with_account(managed_account.id)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[managed_account.account_creation.id]["sf_account"], sf_account.name)

    def test_cost_method(self):
        opportunity = Opportunity.objects.create()
        placement1 = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM)
        placement2 = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV)
        placement3 = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(name="")
        managed_account.managers.add(chf_account)
        Campaign.objects.create(salesforce_placement=placement1, account=managed_account)
        Campaign.objects.create(salesforce_placement=placement2, account=managed_account)
        Campaign.objects.create(salesforce_placement=placement3, account=managed_account)
        account_creation = managed_account.account_creation
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        self.__set_non_admin_user_with_account(managed_account.id)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(
            set(accounts[account_creation.id]["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_cost_client_cost_dashboard(self):
        manager = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID)
        account = Account.objects.create()
        account.managers.add(manager)
        account.save()
        opportunity = Opportunity.objects.create()
        placement_cpm = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=2.)
        placement_cpv = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=2.)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE)
        placement_hard_cost = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=523)
        placement_dynamic_budget = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)
        placement_cpv_rate_and_tech_fee = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=.2)
        placement_cpm_rate_and_tech_fee = OpPlacement.objects.create(
            id=next(str_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=.3)

        campaigns = (
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_cpm, impressions=2323),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_cpv, video_views=321),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_outgoing_fee),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_hard_cost),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_dynamic_budget, cost=412),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_cpv_rate_and_tech_fee,
                video_views=245, cost=32),
            Campaign.objects.create(
                account=account,
                salesforce_placement=placement_cpm_rate_and_tech_fee,
                impressions=632, cost=241)
        )

        client_cost = sum(
            [
                get_client_cost(
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
                for c in campaigns
            ]
        )
        aw_cost = sum([c.cost for c in campaigns])
        self.assertNotEqual(client_cost, aw_cost)

        test_cases = (
            (True, aw_cost),
            (False, client_cost),
        )
        for aw_rates, expected_cost in test_cases:
            user_settings = {
                StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST: aw_rates,
                StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
                StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS: True,
            }
            self.user.perms.update(user_settings)
            self.user.save()
            with self.subTest(**user_settings):
                response = self.client.get(self.url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                accs = dict((acc["id"], acc) for acc in response.data["items"])
                acc_data = accs.get(account.account_creation.id)
                self.assertIsNotNone(acc_data)
                self.assertAlmostEqual(acc_data["cost"], expected_cost)

    def test_demo_brand(self):
        recreate_test_demo_data()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["brand"], DEMO_BRAND)

    def test_demo_cost_type(self):
        recreate_test_demo_data()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["cost_method"], DEMO_COST_METHOD)

    def test_demo_agency(self):
        recreate_test_demo_data()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["sf_account"], DEMO_SF_ACCOUNT)

    def test_list_only_chf_accounts(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        another_mcc_account = Account.objects.create(can_manage_clients=True)
        visible_account = Account.objects.create()
        visible_account.managers.add(chf_mcc_account)
        hidden_account = Account.objects.create()
        hidden_account.managers.add(another_mcc_account)

        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = response.data["items"]
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[0]["id"], visible_account.account_creation.id)

    def test_no_demo_data(self):
        self.user.perms.update({
            StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS: True,
        })
        self.user.save()
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        Campaign.objects.create(account=account)

        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
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
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        account.account_creation.is_paused = False
        account.account_creation.save()
        Campaign.objects.create(account=account)

        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()

        url = "?".join([
            self.url,
            urlencode(dict(status="Paused")),
        ])

        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)

    def test_no_overcalculate_statistic(self):
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        campaign = Campaign.objects.create(
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
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        data = response.data
        self.assertEqual(data["items"][0]["impressions"], campaign.impressions)

    def test_demo_is_first_and_visible(self):
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_DEMO_ACCOUNT] = True
        self.user.save()
        recreate_test_demo_data()
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        items = response.data["items"]
        self.assertEqual([i["id"] for i in items], [DEMO_ACCOUNT_ID, account.account_creation.id])

    def test_invisible_demo_account_permission(self):
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_DEMO_ACCOUNT] = False
        self.user.save()
        recreate_test_demo_data()
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        items = response.data["items"]
        self.assertEqual([i["id"] for i in items], [account.account_creation.id])