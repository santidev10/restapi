from django.core.urlresolvers import reverse
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.add_custom_user_permission(self.user, "view_media_buying")

    @staticmethod
    def create_ad_group_creation(owner):
        account_creation = AccountCreation.objects.create(
            name="", owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
            max_rate="666.666",
        )
        TargetingItem.objects.create(
            ad_group_creation=ad_group_creation,
            criteria="js",
            type=TargetingItem.KEYWORD_TYPE,
            is_negative=True,
        )
        AdCreation.objects.create(
            name="FF",
            ad_group_creation=ad_group_creation,
        )
        return ad_group_creation

    def test_success_fail_has_no_permission(self):
        self.remove_custom_user_permission(self.user, "view_media_buying")

        ac = self.create_ad_group_creation(self.user)
        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ac.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_post(self):
        ac = self.create_ad_group_creation(self.user)
        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ac.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertNotEqual(ac.id, data['id'])

        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name',  'updated_at', 'ad_creations',
                'genders', 'parents', 'age_ranges',
                'targeting', 'max_rate', 'video_ad_format',
            }
        )
        self.assertEqual(data['max_rate'], ac.max_rate)
        self.assertEqual(
            set(data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )
        self.assertEqual(
            set(data['targeting']['keyword']['negative'][0]),
            {'criteria', 'is_negative', 'type', 'name'}
        )
        self.assertEqual(data['name'], "{} (1)".format(ac.name))

    def test_success_post_increment_name(self):
        ag = self.create_ad_group_creation(owner=self.user)
        ag.name = "FF 1 (199)"
        ag.save()
        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ag.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data['name'], "FF 1 (200)")

    def test_success_post_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ad_group.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_duplicate_to_another_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        campaign_creation_1 = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        campaign_creation_2 = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="Test name", campaign_creation=campaign_creation_1,
            max_rate="666.666",
        )

        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ad_group_creation.id,))
        response = self.client.post("{}?to={}".format(url, campaign_creation_2.id))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(campaign_creation_1.ad_group_creations.count(), 1)
        self.assertEqual(campaign_creation_2.ad_group_creations.count(), 1)
        self.assertEqual(data['name'], ad_group_creation.name)

    def test_fail_duplicate_to_another_not_found_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        account_creation_1 = AccountCreation.objects.create(
            name="", owner=get_user_model().objects.create(email="me@text.com"),
        )
        campaign_creation_1 = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        campaign_creation_2 = CampaignCreation.objects.create(
            name="", account_creation=account_creation_1,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="Test name", campaign_creation=campaign_creation_1,
            max_rate="666.666",
        )

        url = reverse("aw_creation_urls:ad_group_creation_duplicate",
                      args=(ad_group_creation.id,))
        response = self.client.post("{}?to={}".format(url, campaign_creation_2.id))

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)


