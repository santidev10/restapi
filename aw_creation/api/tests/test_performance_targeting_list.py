from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo import DEMO_ACCOUNT_ID
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountListAPITestCase(AwReportingAPITestCase):
    details_keys = {
        'id', 'name', 'account', 'status', 'start', 'end', 'is_managed',
        'is_changed', 'weekly_chart', 'thumbnail',
        'video_views', 'cost', 'video_view_rate', 'ctr_v', 'impressions', 'clicks',
        "ad_count", "channel_count", "video_count", "interest_count", "topic_count", "keyword_count",
        "is_disapproved"
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

    def test_marked_is_disapproved_account(self):
        def create_account_creation_with_ad(obj_id, is_disapproved):
            account = Account.objects.create(id=obj_id, name="")
            account_creation = AccountCreation.objects.create(name="", owner=self.user, account=account, )

            campaign = Campaign.objects.create(id=obj_id, name="", account=account, cost=100)
            ad_group = AdGroup.objects.create(id=obj_id, campaign=campaign)
            Ad.objects.create(id=obj_id, ad_group=ad_group, is_disapproved=is_disapproved)
            return account_creation

        account_creation_1 = create_account_creation_with_ad(1, True)
        account_creation_2 = create_account_creation_with_ad(2, False)

        url = reverse("aw_creation_urls:performance_targeting_list")
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 3)
        campaigns = response.data['items']
        self.assertEqual(len(campaigns), 3)

        campaign_map = dict((c['id'], c) for c in campaigns)
        self.assertEqual(campaign_map.keys(), {account_creation_1.id, account_creation_2.id, DEMO_ACCOUNT_ID})
        self.assertFalse(campaign_map[DEMO_ACCOUNT_ID].get('is_disapproved'))
        self.assertTrue(campaign_map[account_creation_1.id].get('is_disapproved'))
        self.assertFalse(campaign_map[account_creation_2.id].get('is_disapproved'))
