from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED

from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase


class AdGroupListAPITestCase(ExtendedAPITestCase):

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

        ag_creation = AdGroupCreation.objects.create(
            name="Wow", campaign_creation=campaign_creation,
        )

        url = reverse("aw_creation_urls:optimization_ad_group_list",
                      args=(campaign_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(
            set(data[0].keys()),
            {
                'id', 'name',
                'is_approved',
                'max_rate',
                'video_url',
                'final_url',
                'targeting',
                'age_ranges',
                'ct_overlay_text',
                'display_url',
                'genders',
                'parents',
                'thumbnail',
            }
        )
        self.assertEqual(data[0]['id'], ag_creation.id)

    def test_success_post(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = datetime.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )

        url = reverse("aw_creation_urls:optimization_ad_group_list",
                      args=(campaign_creation.id,))
        post_data = dict()

        response = self.client.post(
            url, json.dumps(post_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(
            set(response.data.keys()),
            {
                'id', 'name',
                'is_approved',
                'max_rate',
                'video_url',
                'final_url',
                'targeting',
                'age_ranges',
                'ct_overlay_text',
                'display_url',
                'genders',
                'parents',
                'thumbnail',
            }
        )




