from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model
from datetime import datetime, timedelta
from aw_creation.models import *
from aw_reporting.models import *


class AccountListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_data(self, user):
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="", account=account, impressions=10)

        ac_creation = AccountCreation.objects.create(
            name="", owner=user,
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
        return ac_creation

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        self.create_data(user)
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
        self.create_data(self.user)
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
        self.assertEqual(str(item['ordered_cpv']), '0.070000')

    def test_success_get_ended_account(self):
        account_creation = self.create_data(self.user)

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 1)

        past_date = datetime.now() - timedelta(days=2)
        campaign_creation = account_creation.campaign_creations.first()
        campaign_creation.end = past_date
        campaign_creation.save()

        url = reverse("aw_creation_urls:optimization_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 1,
                         "The second campaign-creation has no end date")

        account_creation.campaign_creations.all().update(end=past_date)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 0)

