from datetime import timedelta

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DemoAccount


class AccountCreationSetupAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_all_available(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE,
                                         AdGroupCreation.BUMPER_AD])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_get_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE])

    def test_success_get_pushed_ad_group(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
            video_ad_format=AdGroupCreation.BUMPER_AD,
            sync_at=timezone.now() + timedelta(seconds=2),
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.BUMPER_AD])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.IN_STREAM_TYPE)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_pushed_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            sync_at=timezone.now() + timedelta(seconds=2),
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_two_ads(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_two_ad_groups(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )
        AdCreation.objects.create(
            name="", ad_group_creation=AdGroupCreation.objects.create(
                name="", campaign_creation=campaign_creation,
            ),
        )
        url = reverse("aw_creation_urls:ad_creation_available_ad_formats",
                      args=(ad_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_validate_bumper_video_duration(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
        )

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD,
                            video_duration=7)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup",
                    args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD,
                            video_duration=6)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
