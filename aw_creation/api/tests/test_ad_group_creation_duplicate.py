from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

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
                'targeting', 'max_rate',
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

        ad = data['ad_creations'][0]
        self.assertEqual(
            set(ad.keys()),
            {
                'id', 'updated_at', 'custom_params', 'name', 'tracking_template',
                'video_url', 'display_url', 'final_url', 'thumbnail', 'companion_banner',
                'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title',
            }
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
