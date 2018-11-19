import json
from datetime import timedelta
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, AdCreation
from aw_reporting.demo.models import DemoAccount
from utils.datetime import now_in_default_tz
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher


class AdCreationListAPITestCase(ExtendedAPITestCase):

    detail_keys = {
        'id', 'name', 'updated_at', 'video_url', 'final_url',
        'tracking_template', 'custom_params', 'display_url', 'video_ad_format',
        'companion_banner',
        'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title', 'video_duration',

        "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
        "beacon_view_1", "beacon_view_2", "beacon_view_3",
        "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
        "beacon_first_quartile_1", "beacon_first_quartile_2", "beacon_first_quartile_3",
        "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
        "beacon_third_quartile_1", "beacon_third_quartile_2", "beacon_third_quartile_3",
        "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
        "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
        "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3", "is_disapproved"
    }

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        today = now_in_default_tz().date()
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
        AdCreation.objects.create(
            name="Deleted", ad_group_creation=ad_group, is_deleted=True,
        )
        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_get(self):
        today = now_in_default_tz().date()
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
        AdCreation.objects.create(
            name="Deleted", ad_group_creation=ad_group, is_deleted=True,
        )
        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
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
        today = now_in_default_tz().date()
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




