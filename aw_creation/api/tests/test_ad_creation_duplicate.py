from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DemoAccount


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

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
            beacon_impression_1="http://wtf.com",
            beacon_vast_2="http://feed.me?no=1&yes=0",
        )
        return ad_creation

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        ad = self.create_ad_creation(owner=self.user)
        url = reverse("aw_creation_urls:ad_creation_duplicate",
                      args=(ad.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

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
                'video_url', 'display_url', 'final_url', 'video_ad_format', 'companion_banner',
                'video_id', 'video_title', 'video_description', 'video_thumbnail',
                'video_channel_title', 'video_duration',

                "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
                "beacon_view_1", "beacon_view_2", "beacon_view_3",
                "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
                "beacon_first_quartile_1", "beacon_first_quartile_2", "beacon_first_quartile_3",
                "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
                "beacon_third_quartile_1", "beacon_third_quartile_2", "beacon_third_quartile_3",
                "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
                "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
                "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3", "is_disapproved",
                "headline", "description_1", "description_2",
            }
        )
        self.assertEqual(data['name'], "{} (1)".format(ad.name))
        self.assertEqual(data['beacon_impression_1'], ad.beacon_impression_1)
        self.assertEqual(data['beacon_vast_2'], ad.beacon_vast_2)

        ad_duplicate = AdCreation.objects.get(pk=data["id"])
        for f in AdCreation.tag_changes_field_names:
            self.assertIs(getattr(ad_duplicate, f), f in ("beacon_impression_1_changed", "beacon_vast_2_changed"))

    def test_success_post_increment_name(self):
        ad = self.create_ad_creation(owner=self.user)
        AdCreation.objects.create(
            name="FF 1 (199)",  # add another cloned ad
            ad_group_creation=ad.ad_group_creation,
            video_url="https://www.youtube.com/watch?v=gHviuIGQ8uo",
            display_url="www.gg.com",
            final_url="http://www.gg.com",
            tracking_template="http://custom.com",
            custom_params_raw='[{"name": "name 1", "value": "value 1"}]',
        )

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

    def test_success_duplicate_to_another_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        ad_group_creation_1 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad_group_creation_2 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad = AdCreation.objects.create(name="Whiskey", ad_group_creation=ad_group_creation_1)

        url = reverse("aw_creation_urls:ad_creation_duplicate", args=(ad.id,))
        response = self.client.post("{}?to={}".format(url, ad_group_creation_2.id))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(ad_group_creation_1.ad_creations.count(), 1)
        self.assertEqual(ad_group_creation_2.ad_creations.count(), 1)
        self.assertEqual(data['name'], ad.name)

    def test_fail_duplicate_to_another_not_found_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        account_creation_1 = AccountCreation.objects.create(
            name="", owner=get_user_model().objects.create(email="me@text.com"),
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        campaign_creation_1 = CampaignCreation.objects.create(
            name="", account_creation=account_creation_1,
        )
        ad_group_creation_1 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        ad_group_creation_2 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation_1,
        )
        ad = AdCreation.objects.create(name="Whiskey", ad_group_creation=ad_group_creation_1)

        url = reverse("aw_creation_urls:ad_creation_duplicate", args=(ad.id,))
        response = self.client.post("{}?to={}".format(url, ad_group_creation_2.id))

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
