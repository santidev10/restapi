from datetime import timedelta, datetime
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_202_ACCEPTED

from aw_creation.api.urls.names import Name
from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import DEMO_BRAND, DEMO_COST_METHOD, DEMO_AGENCY
from aw_reporting.models import *
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "id", "name", "account", "status", "start", "end", "is_managed",
        "is_changed", "weekly_chart", "thumbnail",
        "video_views", "cost", "video_view_rate", "ctr_v", "impressions",
        "clicks",
        "ad_count", "channel_count", "video_count", "interest_count",
        "topic_count", "keyword_count",
        "is_disapproved", "from_aw", "updated_at",
        "brand", "cost_method", "agency", "average_cpm", "average_cpv",
        "ctr", "ctr_v"
    }

    url = reverse(Namespace.AW_CREATION + ":" + Name.CreationSetup.ACCOUNT_LIST)

    def setUp(self):
        self.user = self.create_test_user()

    def __set_user_with_account(self, account_id):
        user = self.user
        user.is_staff = True
        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account_id]
        user.aw_settings[UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY] = True
        user.save()

    def test_success_post(self):
        for uid, name in ((1000, "English"), (1003, "Spanish")):
            Language.objects.get_or_create(id=uid, name=name)

        response = self.client.post(self.url)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

        self.assertEqual(
            set(response.data.keys()),
            {
                "id", "name", "account", "campaign_creations",
                "updated_at", "is_ended", "is_paused", "is_approved",
            }
        )

        item = AccountCreation.objects.get(pk=response.data["id"])
        self.assertEqual(item.is_deleted, True)  # item is hidden

        campaign_creation = response.data["campaign_creations"][0]
        self.assertEqual(
            set(campaign_creation.keys()),
            {
                "id", "name", "updated_at", "content_exclusions",
                "start", "end", "budget", "languages", "devices",
                "frequency_capping", "ad_schedule_rules",
                "location_rules", "ad_group_creations",
                "video_networks", "type", "delivery_method",
            }
        )
        self.assertEqual(len(campaign_creation["languages"]), 1)

        ad_group_creation = campaign_creation["ad_group_creations"][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {
                "id", "name", "updated_at", "ad_creations", "max_rate",
                "genders", "parents", "age_ranges", "targeting",
                "video_ad_format",
            }
        )

        self.assertEqual(
            set(ad_group_creation["targeting"].keys()),
            {"channel", "video", "topic", "interest", "keyword"}
        )
        self.assertEqual(len(ad_group_creation["ad_creations"]), 1)

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        AccountCreation.objects.create(
            name="", owner=user,
        )
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
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
        self.assertEqual(response.data["items_count"], 1,
                         "Only Demo account")
        self.assertEqual(len(response.data["items"]), 1)

    def test_success_get(self):
        account = Account.objects.create(id="123", name="")
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=date,
                                              ad_group=ad_group, impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=date,
                                              ad_group=ad_group, impressions=12)

        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, account=account,
        )
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
        # --
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
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

    def test_get_chf_account_creation_list_queryset(self):
        self.user.is_staff = True
        self.user.save()
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        expected_account_id = "1"
        managed_account = Account.objects.create(
            id=expected_account_id, name="")
        managed_account.managers.add(chf_account)
        AccountCreation.objects.create(
            name="Test", owner=self.user, account=managed_account)
        account1 = Account.objects.create(id="2", name="")
        AccountCreation.objects.create(
            name="Test", owner=self.user, account=account1)
        account2 = Account.objects.create(id="3", name="")
        AccountCreation.objects.create(
            name="Test", owner=self.user, account=account2)
        account3 = Account.objects.create(id="4", name="")
        AccountCreation.objects.create(
            name="Test", owner=self.user, account=account3)
        self.__set_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?is_chf=1".format(self.url))
        accounts_ids = {a["account"] for a in response.data["items"]}
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(accounts_ids, {"demo", expected_account_id})

    def test_success_sort_by(self):
        account1 = Account.objects.create(id="123", name="")
        stats = dict(account=account1, name="", impressions=10, video_views=9,
                     clicks=9, cost=9)
        Campaign.objects.create(id=1, **stats)
        Campaign.objects.create(id=2, **stats)
        top_account = AccountCreation.objects.create(
            name="Top account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id="456", name="")
        stats = dict(account=account2, name="", impressions=3, video_views=2,
                     clicks=1, cost=3)
        Campaign.objects.create(id=3, **stats)
        Campaign.objects.create(id=4, **stats)
        AccountCreation.objects.create(
            name="Bottom account", owner=self.user, account=account2,
        )

        # --

        for sort_by in ("impressions", "video_views", "clicks", "cost",
                        "video_view_rate", "ctr_v"):
            with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher):
                response = self.client.get(
                    "{}?sort_by={}".format(self.url, sort_by))
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            expected_top_account = items[1]
            self.assertEqual(top_account.name, expected_top_account["name"])

    def test_success_sort_by_name(self):
        account1 = Account.objects.create(id="123", name="")
        creation_1 = AccountCreation.objects.create(
            name="First account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id="456", name="Second account")
        creation_2 = AccountCreation.objects.create(name="", owner=self.user,
                                                    account=account2,
                                                    is_managed=False,
                                                    is_approved=True)

        creation_3 = AccountCreation.objects.create(name="Third account",
                                                    owner=self.user,
                                                    account=account2)

        # --
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get("{}?sort_by=name".format(self.url))

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]

        self.assertEqual(
            ("Demo", creation_1.name, creation_2.account.name, creation_3.name),
            tuple(a["name"] for a in items)
        )

    def test_success_metrics_filter(self):
        AccountCreation.objects.create(name="Empty", owner=self.user,
                                       is_ended=False, is_paused=False,
                                       is_approved=True)
        account = Account.objects.create(id=1, name="")
        AccountCreation.objects.create(
            name="Maximum", owner=self.user, account=account,
        )
        Campaign.objects.create(
            id=1, name="", account=account,
            impressions=10, video_views=10, clicks=10, cost=10,
        )
        account = Account.objects.create(id=2, name="")
        AccountCreation.objects.create(
            name="Minimum", owner=self.user, account=account,
        )
        Campaign.objects.create(
            id=2, name="", account=account,
            impressions=4, video_views=2, clicks=1, cost=1,
        )
        # --
        test_filters = (
            ("impressions", 1, 5, 2, 11),
            ("video_views", 1, 2, 3, 11),
            ("clicks", 1, 2, 2, 10),
            ("cost", 1, 2, 2, 10),
            ("video_view_rate", 50, 75, 75, 100),
            ("ctr_v", 25, 50, 75, 100),
        )

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        for metric, min1, max1, min2, max2 in test_filters:
            with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 self.patch_user_settings(**user_settings):
                response = self.client.get(
                    "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                        base_url=self.url, metric=metric, min=min1, max=max1)
                )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["items"][-1]["name"], "Minimum")
            for item in response.data["items"]:
                self.assertGreaterEqual(item[metric], min1)
                self.assertLessEqual(item[metric], max1)

            with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 self.patch_user_settings(**user_settings):
                response = self.client.get(
                    "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                        base_url=self.url, metric=metric, min=min2, max=max2)
                )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["items"][-1]["name"], "Maximum")
            for item in response.data["items"]:
                self.assertGreaterEqual(item[metric], min2)
                self.assertLessEqual(item[metric], max2)

    def test_success_status_filter(self):
        AccountCreation.objects.create(name="Pending", owner=self.user)
        AccountCreation.objects.create(name="Ended", owner=self.user,
                                       is_ended=True, is_paused=True,
                                       is_approved=True)
        AccountCreation.objects.create(name="Paused", owner=self.user,
                                       is_ended=False, is_paused=True,
                                       is_approved=True)
        AccountCreation.objects.create(name="Approved", owner=self.user,
                                       is_ended=False, is_paused=False,
                                       is_approved=True)
        AccountCreation.objects.create(
            name="Running", owner=self.user, sync_at=datetime.now(),
        )
        # --
        expected = (
            ("Pending", 1),
            ("Ended", 1),
            ("Paused", 1),
            ("Approved", 1),
            ("Running", 2),  # +DemoAccount
        )

        for status, count in expected:
            with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher):
                response = self.client.get(
                    "{}?show_closed=1&status={}".format(self.url, status))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["items_count"], count)
            self.assertEqual(response.data["items"][-1]["name"], status)
            for i in response.data["items"]:
                self.assertEqual(i["status"], status)

    def test_success_dates_filter(self):
        today = datetime(2015, 1, 1).date()
        max_end, min_start = today, today - timedelta(days=10)

        AccountCreation.objects.create(name="Empty", owner=self.user)
        ac = AccountCreation.objects.create(name="Settings+", owner=self.user)
        CampaignCreation.objects.create(name="", account_creation=ac,
                                        start=min_start, end=max_end)

        ac = AccountCreation.objects.create(name="Settings-", owner=self.user)
        CampaignCreation.objects.create(name="", account_creation=ac,
                                        start=min_start,
                                        end=max_end + timedelta(days=1))

        account = Account.objects.create(id=1, name="")
        Campaign.objects.create(id=1, name="", account=account,
                                start_date=min_start, end_date=max_end)
        AccountCreation.objects.create(name="Improted+", owner=self.user,
                                       account=account)

        account = Account.objects.create(id=2, name="")
        Campaign.objects.create(id=2, name="", account=account,
                                start_date=min_start - timedelta(days=1),
                                end_date=max_end)
        AccountCreation.objects.create(name="Improted-", owner=self.user,
                                       account=account)

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(
                "{}?min_start={}&max_end={}".format(self.url, min_start,
                                                    max_end))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        for item in response.data["items"]:
            self.assertIs(item["name"].endswith("+"), True)

    def test_success_from_aw_filter(self):
        AccountCreation.objects.create(name="", owner=self.user,
                                       is_managed=True)

        ac = AccountCreation.objects.create(name="", owner=self.user,
                                            is_managed=True)
        CampaignCreation.objects.create(name="", account_creation=ac)

        account = Account.objects.create(id=1, name="")
        Campaign.objects.create(id=1, name="", account=account)
        managed_acc = AccountCreation.objects.create(name="", owner=self.user,
                                                     account=account,
                                                     is_managed=False)

        response = self.client.get("{}?from_aw=1".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], managed_acc.id)

    # ended account cases
    def test_success_get_account_no_end_date(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], 2,
            "The account has no end date that's why it's shown"
        )

    def test_success_get_demo(self):
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
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
        self.assertEqual(len(item["weekly_chart"]), 7)

    def test_list_no_deleted_accounts(self):
        AccountCreation.objects.create(
            name="", owner=self.user, is_deleted=True
        )
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(len(response.data["items"]), 1)

    def test_filter_campaigns_count(self):
        AccountCreation.objects.create(name="", owner=self.user)
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="")
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_campaigns_count=1&max_campaigns_count=1".format(
                    self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], ac.id)

    def test_filter_campaigns_count_from_ad_words(self):
        account = Account.objects.create(id=1, name="")
        Campaign.objects.create(id=1, name="", account=account)
        ac = AccountCreation.objects.create(name="", account=account,
                                            owner=self.user, is_managed=False)
        AccountCreation.objects.create(name="", owner=self.user)
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_campaigns_count=1&max_campaigns_count=1".format(
                    self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], ac.id)

    def test_filter_start_date(self):
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="",
                                        start="2017-01-10")

        ac2 = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac2, name="",
                                        start="2017-02-10")
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_start=2017-01-01&max_start=2017-01-31".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], ac.id)

    def test_filter_end_date(self):
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="",
                                        end="2017-01-10")

        ac2 = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac2, name="",
                                        end="2017-02-10")
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_end=2017-01-01&max_end=2017-01-31".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], ac.id)

    def test_success_get_import_historical_accounts(self):
        from aw_reporting.models import AWConnection, \
            AWConnectionToUserRelation, AWAccountPermission, Account

        connection = AWConnection.objects.create(
            email="test@gmail.com",
            refresh_token="1/stxUUgC2fNCe-z1al",
        )
        AWConnectionToUserRelation.objects.create(
            user=self.user,
            connection=connection,
        )
        manager = Account.objects.create(id="1", name="")
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection, account=manager,
        )
        account = Account.objects.create(id="2", name="Weird name")
        account.managers.add(manager)

        # create a few users that also can see it
        for i in range(3):
            user = get_user_model().objects.create(
                email="another{}@mail.au".format(i),
            )
            AccountCreation.objects.create(
                name="", owner=user, account=account,
            )

        # and create an usual running account creation
        created_account = Account.objects.create(id="3", name="")
        created_account.managers.add(manager)
        AccountCreation.objects.create(name="", owner=self.user,
                                       account=created_account)

        # --
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
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
        self.assertEqual(response.data["items_count"], 3)
        self.assertEqual(len(response.data["items"]), 3)
        item = response.data["items"][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(item["name"], account.name)
        self.assertEqual(item["is_managed"], False)

    def test_brand(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="2", name="")
        account_creation = AccountCreation.objects.create(
            name="Test", owner=self.user, account=managed_account)
        managed_account.managers.add(chf_account)
        test_brand = "Test Brand"
        opportunity = Opportunity.objects.create(brand=test_brand)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign = Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        CampaignCreation.objects.create(account_creation=account_creation,
                                        campaign=None)
        self.__set_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?is_chf=1".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[account_creation.id]["brand"], test_brand)

    def test_agency(self):
        agency = Contact.objects.create(first_name="first", last_name="last")
        opportunity = Opportunity.objects.create(agency=agency)
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            salesforce_placement=placement, account=managed_account)
        managed_account.managers.add(chf_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user, account=managed_account)
        CampaignCreation.objects.create(account_creation=account_creation,
                                        campaign=None)
        self.__set_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?is_chf=1".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[account_creation.id]["agency"], agency.name)

    def test_demo_agency(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["agency"], DEMO_AGENCY)

    def test_chf_from_aw_is_null(self):
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        managed_account = Account.objects.create(id="1", name="")
        managed_account.managers.add(chf_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user,
            account=managed_account, is_managed=True)
        self.__set_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?is_chf=1".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(accounts[account_creation.id]["from_aw"], None)

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
        campaign1 = Campaign.objects.create(
            id="1", salesforce_placement=placement1, account=managed_account)
        campaign2 = Campaign.objects.create(
            id="2", salesforce_placement=placement2, account=managed_account)
        campaign3 = Campaign.objects.create(
            id="3", salesforce_placement=placement3, account=managed_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user, account=managed_account)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        CampaignCreation.objects.create(
            account_creation=account_creation, campaign=None)
        self.__set_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?is_chf=1".format(self.url))
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(accounts[account_creation.id]["cost_method"]),
            {p.goal_type for p in [placement1, placement2, placement3]})

    def test_demo_brand(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["brand"], DEMO_BRAND)

    def test_demo_cost_type(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((a["id"], a) for a in response.data["items"])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[DEMO_ACCOUNT_ID]["cost_method"],
                         DEMO_COST_METHOD)

    def test_average_cpm_and_cpv(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        impressions, views, cost = 1, 2, 3
        Campaign.objects.create(account=account,
                                impressions=impressions,
                                video_views=views,
                                cost=cost)
        average_cpv = cost / views
        average_cpm = cost / impressions * 1000
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["average_cpv"], average_cpv)
        self.assertAlmostEqual(acc_data["average_cpm"], average_cpm)

    def test_average_cpm_and_cpv_is_reflect_to_user_settings(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)

        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertNotIn("average_cpv", acc_data)
        self.assertNotIn("average_cpm", acc_data)

        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertIn("average_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)

    def test_ctr_and_ctr_v(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        impressions, views, clicks = 1, 2, 3
        Campaign.objects.create(account=account,
                                impressions=impressions,
                                video_views=views,
                                clicks=clicks)
        ctr = clicks / impressions * 100
        ctr_v = clicks / views * 100
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["ctr"], ctr)
        self.assertAlmostEqual(acc_data["ctr_v"], ctr_v)

    def test_cost_aw_cost(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        costs = (123, 234)
        Campaign.objects.create(id=1, account=account, cost=costs[0])
        Campaign.objects.create(id=2, account=account, cost=costs[1])

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["cost"], sum(costs))

    def test_cost_client_cost(self):
        account = Account.objects.create()
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
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
                tech_fee=c.salesforce_placement.tech_fee
            )
             for c in campaigns]
        )

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["cost"], client_cost)
