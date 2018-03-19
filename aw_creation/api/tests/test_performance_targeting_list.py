from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import *
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "id", "name", "account", "status", "start", "end", "is_managed",
        "is_changed", "weekly_chart", "thumbnail",
        "video_views", "cost", "video_view_rate", "ctr_v", "impressions",
        "clicks",
        "ad_count", "channel_count", "video_count", "interest_count",
        "topic_count", "keyword_count",
        "is_disapproved", "from_aw", "updated_at"
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_post(self):
        url = reverse("aw_creation_urls:performance_targeting_list")
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_405_METHOD_NOT_ALLOWED)

    def test_success_get(self):
        account = Account.objects.create(id="123", name="")
        Campaign.objects.create(id=1, name="", account=account, cost=100)
        ac_creation = AccountCreation.objects.create(
            name="This is a visible account on Performance list", owner=self.user, account=account,
        )
        AccountCreation.objects.create(name="No account", owner=self.user)
        no_delivery_account = Account.objects.create(id="321", name="")
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
        account = Account.objects.create(id="123", name="")
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
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
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
        account_1 = Account.objects.create(id=1)
        account_2 = Account.objects.create(id=2
                                           )
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
