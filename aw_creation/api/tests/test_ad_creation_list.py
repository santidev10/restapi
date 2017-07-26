from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import *
from aw_reporting.demo.models import DemoAccount
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AdCreationListAPITestCase(ExtendedAPITestCase):

    detail_keys = {
        'id', 'name', 'updated_at', 'video_url', 'final_url',
        'tracking_template', 'custom_params', 'display_url', 'thumbnail',
        'companion_banner',
        'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title',
    }

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

        ad_group = AdGroupCreation.objects.create(
            name="Wow", campaign_creation=campaign_creation,
        )
        AdCreation.objects.create(
            name="Mmm", ad_group_creation=ad_group,
        )

        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(campaign_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def perform_get_format_check(self, data):
        self.assertGreater(len(data), 0)
        self.assertEqual(
            set(data[0].keys()),
            self.detail_keys
        )

    def test_success_get_demo(self):
        account = DemoAccount()
        campaign = account.children[0]
        ad_group = campaign.children[0]
        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def test_fail_post_demo(self):
        account = DemoAccount()
        campaign = account.children[0]
        ad_group = campaign.children[0]
        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group.id,))
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
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )

        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group_creation.id,))
        post_data = dict()

        response = self.client.post(
            url, json.dumps(post_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(
            set(response.data.keys()),
            self.detail_keys
        )




