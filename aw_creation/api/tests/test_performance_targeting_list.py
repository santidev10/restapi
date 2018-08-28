from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED

from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from userprofile.models import UserSettingsKey
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountListAPITestCase(AwReportingAPITestCase):
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

    def setUp(self):
        self.user = self.create_test_user()
        self.mcc_account = Account.objects.create(can_manage_clients=True)
        aw_connection = AWConnection.objects.create(refresh_token="token")
        AWAccountPermission.objects.create(aw_connection=aw_connection, account=self.mcc_account)
        AWConnectionToUserRelation.objects.create(connection=aw_connection, user=self.user)

    def test_fail_post(self):
        url = reverse("aw_creation_urls:performance_targeting_list")
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_405_METHOD_NOT_ALLOWED)

    def test_success_get(self):
        account = Account.objects.create(id="123", name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        Campaign.objects.create(id=1, name="", account=account, cost=100)
        ac_creation = AccountCreation.objects.create(
            name="This is a visible account on Performance list", owner=self.user, account=account,
        )
        AccountCreation.objects.create(name="No account", owner=self.user)
        no_delivery_account = Account.objects.create(id="321", name="",
                                                     skip_creating_account_creation=True)
        Campaign.objects.create(id=2, name="", account=no_delivery_account, cost=0)
        AccountCreation.objects.create(name="No delivery account", owner=self.user, account=no_delivery_account)

        # --
        url = reverse("aw_creation_urls:performance_targeting_list")
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 2)
        self.assertEqual(len(response.data['items']), 2)
        item = response.data['items'][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(item["id"], ac_creation.id)

    def test_success_filter_campaign_count(self):
        account = Account.objects.create(id="123", name="",
                                         skip_creating_account_creation=True)
        account.managers.add(self.mcc_account)
        Campaign.objects.create(id=1, name="", account=account, cost=100)
        Campaign.objects.create(id=2, name="", account=account, cost=200)
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, account=account,
        )
        CampaignCreation.objects.create(name="", account_creation=ac_creation)
        CampaignCreation.objects.create(name="", account_creation=ac_creation)

        # --
        url = reverse("aw_creation_urls:performance_targeting_list")
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get("{}?max_campaigns_count=2".format(url))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 2)
        item = response.data['items'][1]
        self.assertEqual(item["id"], ac_creation.id)

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:performance_targeting_list")
        user_settings = {
            UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 1)
        self.assertEqual(len(response.data['items']), 1)
        item = response.data['items'][0]

        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(len(item['weekly_chart']), 7)

    def test_success_from_aw(self):
        account_1 = Account.objects.create(id=1,
                                           skip_creating_account_creation=True)
        account_1.managers.add(self.mcc_account)
        account_2 = Account.objects.create(id=2,
                                           skip_creating_account_creation=True)
        account_2.managers.add(self.mcc_account)
        Campaign.objects.create(id=1, account=account_1, cost=1)
        aw_account = AccountCreation.objects.create(name="From AdWords", owner=self.user, is_managed=False,
                                                    account=account_1)
        Campaign.objects.create(id=2, account=account_2, cost=1)
        internal_account = AccountCreation.objects.create(name="Internal", owner=self.user, is_managed=True,
                                                          account=account_2)

        url = reverse("aw_creation_urls:performance_targeting_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        accounts_map = dict((acc.get('id'), acc) for acc in response.data.get('items'))
        self.assertTrue(accounts_map.get(aw_account.id).get('from_aw'))
        self.assertFalse(accounts_map.get(internal_account.id).get('from_aw'))
