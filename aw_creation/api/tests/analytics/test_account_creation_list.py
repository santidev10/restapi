from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_202_ACCEPTED

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import Language
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class AnalyticsAccountCreationListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "account",
        "ad_count",
        "average_cpm",
        "average_cpv",
        "channel_count",
        "clicks",
        "cost",
        "ctr",
        "ctr_v",
        "ctr_v",
        "end",
        "from_aw",
        "id",
        "impressions",
        "interest_count",
        "is_changed",
        "is_disapproved",
        "is_editable",
        "is_managed",
        "keyword_count",
        "name",
        "plan_cpm",
        "plan_cpv",
        "start",
        "status",
        "thumbnail",
        "topic_count",
        "updated_at",
        "video_count",
        "video_view_rate",
        "video_views",
        "weekly_chart",
    }

    url = reverse(Name.Analytics.ACCOUNT_LIST, [RootNamespace.AW_CREATION, Namespace.ANALYTICS])

    def setUp(self):
        self.user = self.create_test_user()
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
        account = Account.objects.create(id="123", name="",
                                         skip_creating_account_creation=True)
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
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher):
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
        item = response.data["items"][0]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )

    def test_success_sort_by(self):
        account1 = Account.objects.create(id="123", name="",
                                          skip_creating_account_creation=True)
        account1.managers.add(self.mcc_account)
        stats = dict(account=account1, name="", impressions=10, video_views=9,
                     clicks=9, cost=9)
        Campaign.objects.create(id=1, **stats)
        Campaign.objects.create(id=2, **stats)
        top_account = AccountCreation.objects.create(
            name="Top account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id="456", name="",
                                          skip_creating_account_creation=True)
        account2.managers.add(self.mcc_account)
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
            with patch(
                    "aw_creation.api.serializers.SingleDatabaseApiConnector",
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
        account1 = Account.objects.create(id=next(int_iterator), name="",
                                          skip_creating_account_creation=True)
        account1.managers.add(self.mcc_account)
        creation_1 = AccountCreation.objects.create(
            name="First account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id=next(int_iterator), name="Second account",
                                          skip_creating_account_creation=True)
        account2.managers.add(self.mcc_account)
        creation_2 = AccountCreation.objects.create(name="", owner=self.user,
                                                    account=account2,
                                                    is_managed=False,
                                                    is_approved=True)

        account3 = Account.objects.create(id=next(int_iterator), name="Third account",
                                          skip_creating_account_creation=True)
        account3.managers.add(self.mcc_account)
        creation_3 = AccountCreation.objects.create(name="Third account",
                                                    owner=self.user,
                                                    account=account3)

        # --
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?sort_by=name".format(self.url))

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]

        self.assertEqual(
            (
                "Demo", creation_1.name, creation_2.account.name,
                creation_3.name),
            tuple(a["name"] for a in items)
        )

    def test_success_metrics_filter(self):
        AccountCreation.objects.create(name="Empty", owner=self.user,
                                       is_ended=False, is_paused=False,
                                       is_approved=True)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        AccountCreation.objects.create(
            name="Maximum", owner=self.user, account=account,
        )
        Campaign.objects.create(
            id=1, name="", account=account,
            impressions=10, video_views=10, clicks=10, cost=10,
        )
        account = Account.objects.create(id=2, name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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
            with patch(
                    "aw_creation.api.serializers.SingleDatabaseApiConnector",
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

            with patch(
                    "aw_creation.api.serializers.SingleDatabaseApiConnector",
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
        mcc_account = self.mcc_account

        def create_account():
            account = Account.objects.create(id=next(int_iterator), name="",
                                             skip_creating_account_creation=True)
            account.managers.add(mcc_account)
            return account

        AccountCreation.objects.create(name="Pending", owner=self.user, account=create_account())
        AccountCreation.objects.create(name="Ended", owner=self.user, account=create_account(),
                                       is_ended=True, is_paused=True,
                                       is_approved=True)
        AccountCreation.objects.create(name="Paused", owner=self.user, account=create_account(),
                                       is_ended=False, is_paused=True,
                                       is_approved=True)
        AccountCreation.objects.create(name="Approved", owner=self.user, account=create_account(),
                                       is_ended=False, is_paused=False,
                                       is_approved=True)
        AccountCreation.objects.create(
            name="Running", owner=self.user, sync_at=timezone.now(), account=create_account()
        )
        # --
        expected = (
            ("Pending", 1),
            ("Ended", 1),
            ("Paused", 1),
            ("Approved", 1),
            ("Running", 2),  # with Demo
        )

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher):
            for status, count in expected:
                response = self.client.get(
                    "{}?show_closed=1&status={}".format(self.url, status))
                self.assertEqual(response.status_code, HTTP_200_OK)
                self.assertEqual(response.data["items_count"], count)
                self.assertEqual(response.data["items"][-1]["name"], status)
                for i in response.data["items"]:
                    self.assertEqual(i["status"], status)

    def test_success_dates_filter(self):
        mcc_account = self.mcc_account

        def create_account():
            account = Account.objects.create(id=next(int_iterator), name="",
                                             skip_creating_account_creation=True)
            account.managers.add(mcc_account)
            return account

        today = datetime(2015, 1, 1).date()
        max_end, min_start = today, today - timedelta(days=10)
        AccountCreation.objects.create(name="Empty", owner=self.user, account=create_account())
        ac = AccountCreation.objects.create(name="Settings+", owner=self.user, account=create_account())
        CampaignCreation.objects.create(name="", account_creation=ac,
                                        start=min_start, end=max_end)

        ac = AccountCreation.objects.create(name="Settings-", owner=self.user, account=create_account())
        CampaignCreation.objects.create(name="", account_creation=ac,
                                        start=min_start,
                                        end=max_end + timedelta(days=1))

        account = create_account()
        Campaign.objects.create(id=next(int_iterator), name="", account=account,
                                start_date=min_start, end_date=max_end)
        AccountCreation.objects.create(name="Improted+", owner=self.user, account=account)

        account = create_account()
        Campaign.objects.create(id=next(int_iterator), name="", account=account,
                                start_date=min_start - timedelta(days=1),
                                end_date=max_end)
        AccountCreation.objects.create(name="Improted-", owner=self.user, account=account)

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?min_start={}&max_end={}".format(self.url, min_start, max_end))
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

        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        ac_creation = AccountCreation.objects.create(name="", owner=self.user, account=account)
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

    def test_filter_campaigns_count_from_ad_words(self):
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        Campaign.objects.create(id=1, name="", account=account)
        ac = AccountCreation.objects.create(name="", account=account,
                                            owner=self.user, is_managed=False)
        AccountCreation.objects.create(name="", owner=self.user)
        # --
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector", new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(
                "{}?min_campaigns_count=1&max_campaigns_count=1".format(
                    self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], ac.id)

    def test_filter_start_date(self):
        user = self.user

        def add_account(campaign_start):
            account = Account.objects.create(id=next(int_iterator), name="",
                                             skip_creating_account_creation=True)
            account.managers.add(self.mcc_account)
            account_creation = AccountCreation.objects.create(name="", owner=user, account=account)
            CampaignCreation.objects.create(account_creation=account_creation, name="", start=campaign_start)
            return account_creation

        expected_account_creation = add_account("2017-01-10")
        add_account("2017-02-10")
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_start=2017-01-01&max_start=2017-01-31".format(
                    self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], expected_account_creation.id)

    def test_filter_end_date(self):
        user = self.user

        def add_account(campaign_end):
            account = Account.objects.create(id=next(int_iterator), name="",
                                             skip_creating_account_creation=True)
            account.managers.add(self.mcc_account)
            account_creation = AccountCreation.objects.create(name="", owner=user, account=account)
            CampaignCreation.objects.create(account_creation=account_creation, name="", end=campaign_end)
            return account_creation

        expected_account_creation = add_account("2017-01-10")
        add_account("2017-02-10")
        # --
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(
                "{}?min_end=2017-01-01&max_end=2017-01-31".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], expected_account_creation.id)

    def test_chf_is_managed_has_value_on_analytics(self):
        managed_account = Account.objects.create(id="1", name="managed",
                                                 skip_creating_account_creation=True)
        managed_account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user,
            account=managed_account, is_managed=True)
        self.__set_non_admin_user_with_account(managed_account.id)
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((account["id"], account) for account in response.data["items"])
        self.assertIsNotNone(accounts[account_creation.id]["is_managed"])

    def test_average_cpm_and_cpv(self):
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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
        user_settings = {UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True}
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["average_cpv"], average_cpv)
        self.assertAlmostEqual(acc_data["average_cpm"], average_cpm)

    def test_average_cpm_and_cpv_is_reflect_to_user_settings(self):
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)

        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertIn("average_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)

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
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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

    def test_cost_always_aw_cost(self):
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
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
                tech_fee=c.salesforce_placement.tech_fee,
                start=c.start_date,
                end=c.end_date
            )
                for c in campaigns]
        )

        aw_cost = sum([c.cost for c in campaigns])
        self.assertNotEqual(client_cost, aw_cost)

        test_cases = (True, False)

        for aw_rates in test_cases:
            user_settings = {
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rates
            }
            with self.subTest(**user_settings), \
                 self.patch_user_settings(**user_settings):
                response = self.client.get(self.url)

                self.assertEqual(response.status_code, HTTP_200_OK)
                accs = dict((acc["id"], acc) for acc in response.data["items"])
                acc_data = accs.get(account_creation.id)
                self.assertIsNotNone(acc_data)
                self.assertAlmostEqual(acc_data["cost"], aw_cost)

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

        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=1, owner=self.request_user, account=account)
        account_creation.refresh_from_db()

        Campaign.objects.create(id=1, salesforce_placement=placement_cpm,
                                account=account, cost=1, impressions=1)
        Campaign.objects.create(id=2, salesforce_placement=placement_cpv,
                                account=account, cost=1, video_views=1)

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
        self.assertIn("cost", acc_data)
        self.assertIn("plan_cpm", acc_data)
        self.assertIn("plan_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)
        self.assertIn("average_cpv", acc_data)

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
        self.assertIn("cost", acc_data)
        self.assertIn("plan_cpm", acc_data)
        self.assertIn("plan_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)
        self.assertIn("average_cpv", acc_data)

    def test_visible_own_account(self):
        user = self.user
        user.aw_connections.all().delete()
        AccountCreation.objects.create(id=next(int_iterator), owner=user)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)

    def test_created_account_is_managed(self):
        user = self.user
        user.aw_connections.all().delete()
        self.assertFalse(AccountCreation.objects.filter(owner=user).exists())

        response = self.client.post(self.url)

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        account_creation_id = response.data["id"]
        account_creation = AccountCreation.objects.get(id=account_creation_id)
        self.assertTrue(account_creation.is_managed)
        self.assertEqual(account_creation.owner, user)

    def test_visible_linked_accounts(self):
        account = Account.objects.create(id=next(int_iterator), can_manage_clients=False,
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account.save()
        account_creation = AccountCreation.objects.create(id=next(int_iterator), owner=None, account=account)
        account_creation.refresh_from_db()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        self.assertEqual(response.data["items"][1]["id"], account_creation.id)

    def test_is_editable(self):
        user = self.user
        mcc_account = self.mcc_account
        visible_account = Account.objects.create(id=next(int_iterator),
                                                 skip_creating_account_creation=True)
        visible_account.managers.add(mcc_account)
        visible_account_creation = AccountCreation.objects.create(id=next(int_iterator), owner=None,
                                                                  account=visible_account)
        own_account_creation = AccountCreation.objects.create(id=next(int_iterator), account=None, owner=user)
        visible_account_creation.refresh_from_db()
        own_account_creation.refresh_from_db()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 3)
        accounts_by_id = {acc["id"]: acc for acc in response.data["items"]}
        self.assertTrue(accounts_by_id.get(own_account_creation.id)["is_editable"])
        self.assertFalse(accounts_by_id.get(visible_account_creation.id)["is_editable"])

    def test_no_demo_data_on_real_account(self):
        account = Account.objects.create(id=next(int_iterator))
        AccountCreation.objects.filter(account=account).update(owner=self.user)
        Campaign.objects.create(id=next(int_iterator), account=account)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        items = {i["id"]: i for i in response.data["items"]}
        item = items.get(account.account_creation.id)
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

    def test_visible_all_accounts_does_not_affect_values(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/VIQ-223
        Summary: Analytics > The adjustment of OPs visibility affects Analytics data displaying
        """
        account = Account.objects.create(id=next(int_iterator))
        AccountCreation.objects.filter(account=account).update(owner=self.user)
        common_rates = dict(ordered_units=1, total_cost=1)
        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPV,
                                                   **common_rates)
        placement_cpm = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPM,
                                                   **common_rates)
        common_stats = dict(
            clicks=1, cost=1, video_views=1, impressions=1
        )
        Campaign.objects.create(id=next(int_iterator), account=account,
                                salesforce_placement=placement_cpm,
                                **common_stats)
        Campaign.objects.create(id=next(int_iterator), account=account,
                                salesforce_placement=placement_cpv,
                                **common_stats)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: ["some_account_id"],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        items = {i["id"]: i for i in response.data["items"]}
        item = items.get(account.account_creation.id)
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
        for key in stats + rates:
            with self.subTest(key):
                self.assertIsNotNone(item[key], key)
                self.assertGreater(item[key], 0)

    def test_demo_account_visibility_does_not_affect_result(self):
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: False,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        item = response.data["items"][0]
        self.assertEqual(item["id"], DEMO_ACCOUNT_ID)
