import json
from datetime import timedelta

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.test_case import ExtendedAPITestCase


class AdCreationListAPITestCase(ExtendedAPITestCase):
    detail_keys = {
        "id", "name", "updated_at", "video_url", "final_url",
        "tracking_template", "custom_params", "display_url", "video_ad_format",
        "companion_banner",
        "video_id", "video_title", "video_description", "video_thumbnail", "video_channel_title", "video_duration",

        "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
        "beacon_view_1", "beacon_view_2", "beacon_view_3",
        "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
        "beacon_first_quartile_1", "beacon_first_quartile_2", "beacon_first_quartile_3",
        "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
        "beacon_third_quartile_1", "beacon_third_quartile_2", "beacon_third_quartile_3",
        "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
        "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
        "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3", "is_disapproved",
        "headline", "description_1", "description_2", "long_headline", "short_headline", "business_name"
    }

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.MEDIA_BUYING: True,
        })

    def test_success_fail_has_no_permission(self):
        self.user.perms.update({
            StaticPermissions.MEDIA_BUYING: False,
        })
        self.user.save()

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
        recreate_test_demo_data()
        ad_group = AdGroupCreation.objects.filter(ad_group__campaign__account_id=DEMO_ACCOUNT_ID).first()
        url = reverse("aw_creation_urls:ad_creation_list_setup",
                      args=(ad_group.id,))

        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def test_fail_post_demo(self):
        recreate_test_demo_data()
        ad_group = AdGroupCreation.objects.filter(ad_group__campaign__account_id=DEMO_ACCOUNT_ID).first()
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
            url, json.dumps(post_data), content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(
            set(response.data.keys()),
            self.detail_keys
        )
