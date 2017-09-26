from django.core.urlresolvers import reverse
from django.utils import timezone
from datetime import timedelta
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DemoAccount


class AccountCreationSetupAPITestCase(AwReportingAPITestCase):

    def test_success_all_available(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
        self.assertEqual(response.data, [AdGroupCreation.IN_STREAM_TYPE, AdGroupCreation.BUMPER_AD])

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_get_demo(self):
        self.create_test_user()
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
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.IN_STREAM_TYPE)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_pushed_campaign(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_two_ads(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_two_ad_groups(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_validate_bumper_video_duration(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=user,
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
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD, video_duration=7)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        response = self.client.patch(
            reverse("aw_creation_urls:ad_creation_setup", args=(ad_creation.id,)),
            json.dumps(dict(video_ad_format=AdGroupCreation.BUMPER_AD, video_duration=6)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
