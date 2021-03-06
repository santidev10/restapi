# pylint: disable=too-many-lines
from datetime import date
from datetime import datetime
from datetime import timedelta

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
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse


class AnalyticsAccountCreationListAPITestCase(AwReportingAPITestCase, ESTestCase):
    details_keys = {
        "account",
        "all_conversions",
        "average_cpm",
        "average_cpv",
        "clicks",
        "cost",
        "ctr",
        "ctr_v",
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
    }

    url = reverse(Name.Analytics.ACCOUNT_LIST, [RootNamespace.AW_CREATION, Namespace.ANALYTICS])

    def setUp(self):
        super(AnalyticsAccountCreationListAPITestCase, self).setUp()
        self.user = self.create_test_user(perms={StaticPermissions.MANAGED_SERVICE: True,})
        self.mcc_account = Account.objects.create(id=next(int_iterator), can_manage_clients=True)
        aw_connection = AWConnection.objects.create(refresh_token="token")
        AWAccountPermission.objects.create(aw_connection=aw_connection, account=self.mcc_account)
        AWConnectionToUserRelation.objects.create(connection=aw_connection, user=self.user)

    def __set_non_admin_user_with_account(self, account_id):
        user = self.user
        user.perms.update({
            StaticPermissions.ADMIN: False,
        })
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
                "updated_at", "is_ended", "is_paused", "is_approved", "sync_at"
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
                "is_draft", "bid_strategy_type", "sync_at", "target_cpa"
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
        self.assertEqual(response.data["items_count"], 0)
        self.assertEqual(len(response.data["items"]), 0)

    def test_success_get(self):
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        campaign = Campaign.objects.create(id=next(int_iterator), name="", account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        action_date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=action_date,
                                              ad_group=ad_group,
                                              impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=action_date,
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

    def test_success_sort_by(self):
        account1 = Account.objects.create(id=next(int_iterator), name="",
                                          skip_creating_account_creation=True)
        account1.managers.add(self.mcc_account)
        stats = dict(account=account1, name="", impressions=10, video_views=9,
                     clicks=9, cost=9)
        Campaign.objects.create(id=next(int_iterator), **stats)
        Campaign.objects.create(id=next(int_iterator), **stats)
        top_account = AccountCreation.objects.create(
            name="Top account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id=next(int_iterator), name="",
                                          skip_creating_account_creation=True)
        account2.managers.add(self.mcc_account)
        stats = dict(account=account2, name="", impressions=3, video_views=2,
                     clicks=1, cost=3)
        Campaign.objects.create(id=next(int_iterator), **stats)
        Campaign.objects.create(id=next(int_iterator), **stats)
        AccountCreation.objects.create(
            name="Bottom account", owner=self.user, account=account2,
        )

        # --

        for sort_by in ("impressions", "video_views", "clicks", "cost",
                        "video_view_rate", "ctr_v"):
            response = self.client.get(
                "{}?sort_by={}".format(self.url, sort_by))
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            expected_top_account = items[0]
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
        response = self.client.get("{}?sort_by=name".format(self.url))

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]

        self.assertEqual(
            (
                creation_1.name, creation_2.account.name,
                creation_3.name),
            tuple(a["name"] for a in items)
        )

    def test_success_metrics_filter(self):
        AccountCreation.objects.create(name="Empty", owner=self.user,
                                       is_ended=False, is_paused=False,
                                       is_approved=True)
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        AccountCreation.objects.create(
            name="Maximum", owner=self.user, account=account,
        )
        campaign = Campaign.objects.create(
            id=1, name="", account=account,
            impressions=10, video_views=10, clicks=10, cost=10
        )
        ad_group_1 = AdGroup.objects.create(
            id=next(int_iterator), campaign=campaign, cost=10)
        AdGroupStatistic.objects.create(
            date=date(2018, 1, 1), ad_group=ad_group_1,
            cost=10, average_position=1, all_conversions=10)
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        AccountCreation.objects.create(
            name="Minimum", owner=self.user, account=account,
        )
        campaign_2 = Campaign.objects.create(
            id=next(int_iterator), name="", account=account,
            impressions=4, video_views=2, clicks=1, cost=1
        )
        ad_group_2 = AdGroup.objects.create(
            id=next(int_iterator), campaign=campaign_2, cost=1)
        AdGroupStatistic.objects.create(
            date=date(2018, 1, 1), ad_group=ad_group_2,
            cost=1, average_position=1, all_conversions=5)
        # --
        test_filters = (
            ("impressions", 1, 5, 2, 11),
            ("video_views", 1, 2, 3, 11),
            ("clicks", 1, 2, 2, 10),
            ("cost", 1, 2, 2, 10),
            ("video_view_rate", 50, 75, 75, 100),
            ("ctr_v", 25, 50, 75, 100),
            ("all_conversions", 1, 6, 6, 11)
        )

        self.user.perms[StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST] = True
        self.user.save()
        for metric, min1, max1, min2, max2 in test_filters:
            response = self.client.get(
                "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                    base_url=self.url, metric=metric, min=min1, max=max1)
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["items"][-1]["name"], "Minimum")
            for item in response.data["items"]:
                self.assertGreaterEqual(item[metric], min1)
                self.assertLessEqual(item[metric], max1)

            response = self.client.get(
                "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                    base_url=self.url, metric=metric, min=min2, max=max2)
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["items"][-1]["name"], "Maximum")
            for item in response.data["items"]:
                self.assertGreaterEqual(item[metric], min2)
                self.assertLessEqual(item[metric], max2)

    # pylint: disable=too-many-statements
    def test_status_filter(self):
        mcc_account = self.mcc_account

        def create_account():
            account = Account.objects.create(id=next(int_iterator), name="", skip_creating_account_creation=True)
            account.managers.add(mcc_account)
            return account

        # ended
        ended_account = create_account()
        Campaign.objects.create(name="ended 1", id=next(int_iterator), account=ended_account, status="ended")
        Campaign.objects.create(name="ended 2", id=next(int_iterator), account=ended_account, status="ended")
        Campaign.objects.create(name="ended 3", id=next(int_iterator), account=ended_account, status="ended")
        ended_account_creation = AccountCreation.objects.create(
            name="Ended", owner=self.user, account=ended_account, is_approved=True, sync_at=timezone.now())
        # paused
        paused_account = create_account()
        Campaign.objects.create(name="paused 1", id=next(int_iterator), account=paused_account, status="paused")
        Campaign.objects.create(name="serving", id=next(int_iterator), account=paused_account, status="removed")
        Campaign.objects.create(name="paused 2", id=next(int_iterator), account=paused_account, status="paused")
        paused_account_creation = AccountCreation.objects.create(
            name="Paused", owner=self.user, account=paused_account, is_approved=True, sync_at=timezone.now())
        # running
        running_account = create_account()
        Campaign.objects.create(name="paused 1", id=next(int_iterator), account=running_account, status="removed")
        Campaign.objects.create(name="serving", id=next(int_iterator), account=running_account, status="serving")
        Campaign.objects.create(name="paused 2", id=next(int_iterator), account=running_account, status="paused")
        Campaign.objects.create(name="paused 2", id=next(int_iterator), account=running_account, status="ended")
        running_account_creation = AccountCreation.objects.create(
            name="Running", owner=self.user, account=running_account, sync_at=timezone.now(), is_approved=True)
        # pending
        pending_account = create_account()
        pending_account_creation = AccountCreation.objects.create(
            name="Pending", owner=self.user, account=pending_account, is_approved=True)
        # draft
        draft_account_creation = AccountCreation.objects.create(name="Draft", owner=self.user)
        # Ended
        response = self.client.get("{}?status={}".format(self.url, "Ended"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 1)
        self.assertEqual(response.data.get("items")[0].get("id"), ended_account_creation.id)
        ended_account_creation.refresh_from_db()
        self.assertEqual(ended_account_creation.status, AccountCreation.STATUS_ENDED)
        # Paused
        response = self.client.get("{}?status={}".format(self.url, "Paused"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 1)
        self.assertEqual(response.data.get("items")[0].get("id"), paused_account_creation.id)
        paused_account_creation.refresh_from_db()
        self.assertEqual(paused_account_creation.status, AccountCreation.STATUS_PAUSED)
        # Running
        response = self.client.get("{}?status={}".format(self.url, "Running"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 1)
        self.assertEqual(response.data.get("items")[0].get("id"), running_account_creation.id)
        running_account_creation.refresh_from_db()
        self.assertEqual(running_account_creation.status, AccountCreation.STATUS_RUNNING)
        # Pending
        response = self.client.get("{}?status={}".format(self.url, "Pending"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 1)
        self.assertEqual(response.data.get("items")[0].get("id"), pending_account_creation.id)
        pending_account_creation.refresh_from_db()
        self.assertEqual(pending_account_creation.status, AccountCreation.STATUS_PENDING)
        # Draft
        response = self.client.get("{}?status={}".format(self.url, "Draft"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 1)
        self.assertEqual(response.data.get("items")[0].get("id"), draft_account_creation.id)
        draft_account_creation.refresh_from_db()
        self.assertEqual(draft_account_creation.status, AccountCreation.STATUS_DRAFT)

    # pylint: enable=too-many-statements

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

        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        Campaign.objects.create(id=next(int_iterator), name="", account=account)
        managed_acc = AccountCreation.objects.create(name="", owner=self.user,
                                                     account=account,
                                                     is_managed=False)

        response = self.client.get("{}?from_aw=1".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], managed_acc.id)

    # ended account cases
    def test_success_get_account_no_end_date(self):
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        ac_creation = AccountCreation.objects.create(name="", owner=self.user, account=account)
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], 1,
            "The account has no end date that's why it's shown"
        )

    def test_success_get_demo(self):
        recreate_test_demo_data()
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
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 0)
        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_campaigns_count_from_ad_words(self):
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        Campaign.objects.create(id=next(int_iterator), name="", account=account)
        ac = AccountCreation.objects.create(name="", account=account,
                                            owner=self.user, is_managed=False)
        AccountCreation.objects.create(name="", owner=self.user)
        # --
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
        response = self.client.get(
            "{}?min_end=2017-01-01&max_end=2017-01-31".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], expected_account_creation.id)

    def test_chf_is_managed_has_value_on_analytics(self):
        managed_account = Account.objects.create(id=next(int_iterator), name="managed",
                                                 skip_creating_account_creation=True)
        managed_account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            name="1", owner=self.user,
            account=managed_account, is_managed=True)
        self.__set_non_admin_user_with_account(managed_account.id)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts = dict((account["id"], account) for account in response.data["items"])
        self.assertIsNotNone(accounts[account_creation.id]["is_managed"])

    def test_average_cpm_and_cpv(self):
        self.user.perms[StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST] = True
        self.user.save()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), owner=self.request_user, account=account)
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
        self.user.perms[StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS] = True
        self.user.save()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        Campaign.objects.create(account=account)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertIn("average_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)

        # show
        self.user.perms[StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS] = False
        self.user.save()
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertIn("average_cpv", acc_data)
        self.assertIn("average_cpm", acc_data)

    def test_ctr_and_ctr_v(self):
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), owner=self.request_user, account=account)
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
        self.user.perms[StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST] = True
        self.user.save()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), owner=self.request_user, account=account)
        account_creation.refresh_from_db()
        costs = (123, 234)
        Campaign.objects.create(id=next(int_iterator), account=account, cost=costs[0])
        Campaign.objects.create(id=next(int_iterator), account=account, cost=costs[1])

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accs = dict((acc["id"], acc) for acc in response.data["items"])
        acc_data = accs.get(account_creation.id)
        self.assertIsNotNone(acc_data)
        self.assertAlmostEqual(acc_data["cost"], sum(costs))

    def test_cost_always_aw_cost(self):
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), owner=self.request_user, account=account)
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
                id=next(int_iterator), account=account,
                salesforce_placement=placement_cpm, impressions=2323),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
                salesforce_placement=placement_cpv, video_views=321),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
                salesforce_placement=placement_outgoing_fee),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
                salesforce_placement=placement_hard_cost),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
                salesforce_placement=placement_dynamic_budget, cost=412),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
                salesforce_placement=placement_cpv_rate_and_tech_fee,
                video_views=245, cost=32),
            Campaign.objects.create(
                id=next(int_iterator), account=account,
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
                for c in campaigns]
        )

        aw_cost = sum([c.cost for c in campaigns])
        self.assertNotEqual(client_cost, aw_cost)

        test_cases = (True, False)

        for aw_rates in test_cases:
            user_settings = {
                StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST: aw_rates
            }
            with self.subTest(**user_settings):
                self.user.perms.update(user_settings)
                self.user.save()
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
            id=next(int_iterator), owner=self.request_user, account=account)
        account_creation.refresh_from_db()

        Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement_cpm,
                                account=account, cost=1, impressions=1)
        Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement_cpv,
                                account=account, cost=1, video_views=1)

        # show
        self.user.perms[StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS] = True
        self.user.save()
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
        self.user.perms[StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS] = False
        self.user.save()
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
        self.assertEqual(response.data["items_count"], 1)

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
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], account_creation.id)

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
        self.assertEqual(response.data["items_count"], 2)
        accounts_by_id = {acc["id"]: acc for acc in response.data["items"]}
        self.assertTrue(accounts_by_id.get(own_account_creation.id)["is_editable"])
        self.assertFalse(accounts_by_id.get(visible_account_creation.id)["is_editable"])

    def test_visible_all_accounts_does_not_affect_values(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/VIQ-223
        Summary: Analytics > The adjustment of OPs visibility affects Analytics data displaying
        """
        user = self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: False,
        })
        account = Account.objects.create(id=next(int_iterator))
        AccountCreation.objects.filter(account=account).update(owner=user)
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
            UserSettingsKey.VISIBLE_ACCOUNTS: ["some_account_id"],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
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
        recreate_test_demo_data()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = False
        self.user.save()

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        item = response.data["items"][0]
        self.assertEqual(item["id"], DEMO_ACCOUNT_ID)

    def test_demo_is_editable(self):
        recreate_test_demo_data()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = False
        self.user.save()
        response = self.client.get(self.url)

        item = response.data["items"][0]
        self.assertEqual(item["is_editable"], True)

    def test_demo_is_first(self):
        recreate_test_demo_data()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        AccountCreation.objects.create(account=account, owner=self.user)
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        items = response.data["items"]
        self.assertEqual([i["id"] for i in items], [DEMO_ACCOUNT_ID, account.account_creation.id])
