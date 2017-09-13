from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import *
from aw_reporting.demo.models import DemoAccount
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AdGroupListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        today = datetime.now().date()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )

        AdGroupCreation.objects.create(
            name="Wow", campaign_creation=campaign_creation,
        )

        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def perform_get_format_check(self, data):
        self.assertGreater(len(data), 0)
        self.assertEqual(
            set(data[0].keys()),
            {
                'id', 'name', 'targeting', 'updated_at',
                'age_ranges', 'genders', 'parents',
                'ad_creations', 'max_rate', 'video_ad_format',
            }
        )

    def test_success_get_demo(self):
        account = DemoAccount()
        campaign = account.children[0]
        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def test_fail_post_demo(self):
        account = DemoAccount()
        campaign = account.children[0]
        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_post(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = datetime.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )

        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.perform_get_format_check([response.data])




