from datetime import timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.utittests.test_case import ExtendedAPITestCase


class AdGroupListAPITestCase(ExtendedAPITestCase):

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

        AdGroupCreation.objects.create(
            name="Wow", campaign_creation=campaign_creation,
        )
        AdGroupCreation.objects.create(
            name="Deleted", campaign_creation=campaign_creation, is_deleted=True,
        )

        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))

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

        AdGroupCreation.objects.create(
            name="Wow", campaign_creation=campaign_creation,
        )
        AdGroupCreation.objects.create(
            name="Deleted", campaign_creation=campaign_creation, is_deleted=True,
        )

        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
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
        recreate_demo_data()
        campaign_creation = CampaignCreation.objects.filter(campaign__account_id=DEMO_ACCOUNT_ID).first()
        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def test_fail_post_demo(self):
        recreate_demo_data()
        campaign_creation = CampaignCreation.objects.filter(campaign__account_id=DEMO_ACCOUNT_ID).first()
        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
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

        url = reverse("aw_creation_urls:ad_group_creation_list_setup",
                      args=(campaign_creation.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.perform_get_format_check([response.data])




