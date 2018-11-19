from django.core.urlresolvers import reverse
from django.test import override_settings
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import Campaign, Account, User, Opportunity, \
    OpPlacement, SalesForceGoalType, goal_type_str
from aw_reporting.models import CampaignStatistic
from aw_reporting.models.salesforce_constants import ALL_SALESFORCE_REGIONS, \
    salesforce_region_str
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz


class GlobalTrendsFiltersTestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.GlobalTrends.FILTERS)
    expected_keys = {"accounts", "dimension", "indicator", "breakdown", "am",
                     "ad_ops", "sales", "brands", "goal_types", "categories",
                     "region"}
    account_keys = {"id", "name", "start_date", "end_date", "campaigns"}

    def test_authentication_required(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_global_accounts(self):
        self.create_test_user()

        def new_account(new_id):
            return Account.objects.create(id=new_id)

        manager = new_account(1)
        child_account = new_account(2)
        child_account.managers.add(manager)
        child_account.save()
        child_account.refresh_from_db()

        campaign = Campaign.objects.create(
            name="",
            account=child_account
        )
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=now_in_default_tz())

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = response.data.get("accounts", [])
        self.assertEqual(len(accounts), 1)
        account = accounts[0]
        self.assertEqual(account["id"], child_account.id)

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        manager = account.managers.first()
        for i in range(1, 3):
            campaign = Campaign.objects.create(
                id=i,
                name="",
                account=account,
                impressions=1
            )
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=now_in_default_tz())

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(
            set(response.data.keys()),
            self.expected_keys
        )
        self.assertEqual(len(response.data["accounts"]), 1)
        account_data = response.data["accounts"][0]
        self.assertEqual(
            set(account_data.keys()),
            self.account_keys
        )
        self.assertEqual(account_data["id"], account.id)
        self.assertEqual(len(account_data["campaigns"]), 2)
        self.assertEqual(
            set(account_data["campaigns"][0].keys()),
            {
                "id",
                "name",
                "start_date",
                "end_date",
            }
        )

    def test_demo_account(self):
        self.create_test_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(set(response.data.keys()), self.expected_keys)
        self.assertEqual(len(response.data["accounts"]), 1)
        account_data = response.data["accounts"][0]
        self.assertEqual(
            set(account_data.keys()),
            self.account_keys
        )
        self.assertEqual(account_data["id"], DemoAccount().id)
        self.assertEqual(len(account_data["campaigns"]), 2)
        self.assertEqual(
            set(account_data["campaigns"][0].keys()),
            {
                "id",
                "name",
                "start_date",
                "end_date",
            }
        )

    def test_account_managers(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_account_manager = User.objects.create(id="123",
                                                   name="Test User Name",
                                                   is_active=True)
        expected_am_data = dict(id=test_account_manager.id,
                                name=test_account_manager.name)

        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                account_manager=test_account_manager)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz(),
            )

            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("am", [])
        self.assertEqual(account_managers, [expected_am_data])

    def test_account_managers_inactive(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_account_manager = User.objects.create(id="123",
                                                   name="Test User Name",
                                                   is_active=False)

        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                account_manager=test_account_manager)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz(),
            )

            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("am", [])
        self.assertEqual(account_managers, [])

    def test_ad_ops_manager(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_ad_ops = User.objects.create(id="123",
                                          name="Test User Name",
                                          is_active=True)
        expected_ad_ops_data = dict(id=test_ad_ops.id,
                                    name=test_ad_ops.name)

        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                ad_ops_manager=test_ad_ops)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz(),
            )
            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("ad_ops", [])
        self.assertEqual(account_managers, [expected_ad_ops_data])

    def test_ad_ops_manager_inactive(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_ad_ops = User.objects.create(id="123",
                                          name="Test User Name (inactive)",
                                          is_active=False)
        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                ad_ops_manager=test_ad_ops)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz(),
            )
            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("ad_ops", [])
        self.assertEqual(account_managers, [])

    def test_sales_manager(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_sales = User.objects.create(id="123",
                                         name="Test User Name",
                                         is_active=True)
        expected_sales_data = dict(id=test_sales.id,
                                   name=test_sales.name)

        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                sales_manager=test_sales)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz())
            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("sales", [])
        self.assertEqual(account_managers, [expected_sales_data])

    def test_sales_manager_inactive(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_sales = User.objects.create(id="123",
                                         name="Test User Name",
                                         is_active=False)

        def create_relations(_id):
            opportunity = Opportunity.objects.create(
                id=_id,
                sales_manager=test_sales)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz())
            test_account.managers.add(manager)
            test_account.save()

        create_relations(1)
        create_relations(2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_managers = response.data.get("sales", [])
        self.assertEqual(account_managers, [])

    def test_brand(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_brand_1 = "test brand 1"
        test_brand_2 = "test brand 2"
        test_brands = sorted([test_brand_1, test_brand_2])
        expected_brands = [dict(id=b, name=b) for b in test_brands]

        def create_relations(_id, brand):
            opportunity = Opportunity.objects.create(
                id=_id, brand=brand)
            placement = OpPlacement.objects.create(id=_id,
                                                   opportunity=opportunity)
            test_account = Account.objects.create(id=_id)
            campaign = Campaign.objects.create(
                id=_id,
                salesforce_placement=placement,
                account=test_account)
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=now_in_default_tz()
            )

            test_account.managers.add(manager)
            test_account.save()

        create_relations(1, test_brand_1)
        create_relations(2, test_brand_1)
        create_relations(3, test_brand_2)

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["brands"], expected_brands)

    def test_goal_types(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        test_account = Account.objects.create()
        test_account.managers.add(manager)
        test_account.save()
        expected_types = sorted(
            [SalesForceGoalType.CPM, SalesForceGoalType.CPV])
        expected_goal_types = [dict(id=t, name=goal_type_str(t))
                               for t in expected_types]

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=manager.id):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["goal_types"], expected_goal_types)

    def test_region(self):
        self.create_test_user()
        expected_regions = [dict(id=region_id,
                                 name=salesforce_region_str(region_id))
                            for region_id in ALL_SALESFORCE_REGIONS]

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["region"], expected_regions)
