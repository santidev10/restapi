from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def create_ad_creation(owner):
        account_creation = AccountCreation.objects.create(
            name="", owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="FF 1",
            ad_group_creation=ad_group_creation,
            video_url="https://www.youtube.com/watch?v=gHviuIGQ8uo",
            display_url="www.gg.com",
            final_url="http://www.gg.com",
            tracking_template="http://custom.com",
            custom_params_raw='[{"name": "name 1", "value": "value 1"}]',
        )
        return ad_creation

    def test_success_post(self):
        ad = self.create_ad_creation(owner=self.user)
        url = reverse("aw_creation_urls:ad_creation_duplicate",
                      args=(ad.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertNotEqual(ad.id, data['id'])
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'updated_at', 'custom_params', 'name', 'tracking_template',
                'video_url', 'display_url', 'final_url', 'thumbnail', 'companion_banner',
                'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title',
            }
        )
        self.assertEqual(data['name'], "{} (1)".format(ad.name))

    def test_success_post_increment_name(self):
        ad = self.create_ad_creation(owner=self.user)
        ad.name = "FF 1 (199)"
        ad.save()
        url = reverse("aw_creation_urls:ad_creation_duplicate",
                      args=(ad.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data['name'], "FF 1 (200)")

    def test_success_post_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]

        url = reverse("aw_creation_urls:ad_creation_duplicate",
                      args=(ad.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
