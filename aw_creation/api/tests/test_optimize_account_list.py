from datetime import datetime, timedelta

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from urllib.parse import urlencode
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase


class AccountListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        AccountCreation.objects.create(
            name="", owner=user,
        )
        url = reverse("aw_creation_urls:optimization_account_list")
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
        self.assertEqual(response.data['items_count'], 0)
        self.assertEqual(len(response.data['items']), 0)

    def test_success_get(self):
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="", account=account, impressions=10)

        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        camp_creation = CampaignCreation.objects.create(
            name="", campaign=campaign, account_creation=ac_creation,
            goal_units=100, max_rate="0.07",
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
            max_rate="0.07",
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
            max_rate="0.05",
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation, campaign=None,
        )
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
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
            {
                'id', 'name',
                'is_ended',
                'start',
                'ordered_cpv',
                'cpv',
                'ordered_impressions_cost',
                'ordered_views_cost',
                'impressions',
                'views',
                'ordered_views',
                'impressions_cost',
                'is_approved',
                'end',
                'cpm',
                'ordered_cpm',
                'is_optimization_active',
                'is_paused',
                'is_changed',
                'ordered_impressions',
                'views_cost',
            }
        )
        self.assertEqual(item['impressions'], 10)
        self.assertEqual(item['ordered_views'], 100)
        self.assertEqual(item['ordered_views_cost'], 7)
        self.assertEqual(float(item['ordered_cpv']), 0.07)

    # ended account cases
    def test_success_get_account_no_end_date(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 1,
            "The account has no end date that's why it's shown"
        )

    def test_hide_account_end_in_past(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() - timedelta(days=1),
        )

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 0,
            "The only campaign with end date ended yesterday"
        )

    def test_hide_account_end_in_past_two_campaigns(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() - timedelta(days=1),
        )

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 0,
            "The account isn't shown because the only date is in the past"
        )

    def test_hide_account_is_ended_true(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, is_ended=True
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() + timedelta(days=1),
        )

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 0,
            "The account isn't shown because of the flag 'is_ended' "
        )

    def test_show_hidden_account(self):
        AccountCreation.objects.create(
            name="A", owner=self.user, is_ended=True
        )
        live_account = AccountCreation.objects.create(
            name="B", owner=self.user,
        )
        base_url = reverse("aw_creation_urls:optimization_account_list")
        url = "{}?{}".format(
            base_url,
            urlencode(dict(
                showClosed="true",
                sort_by="name",
            )),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 2)
        self.assertEqual(response.data['items'][0]['name'],
                         live_account.name)

