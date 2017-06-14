from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase


class CreationOptionsAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_create(self):
        url = reverse("aw_creation_urls:creation_options")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'name',
                'ad_group_count',
                'video_ad_format',
                'campaign_count',
                'budget',
                'location_rules',
                'devices',
                'goal_type',
                'end',
                'max_rate',
                'languages',
                'goal_units',
                'frequency_capping',
                'start',
                'ct_overlay_text',
                'video_url',
                'final_url',
                'age_ranges',
                'display_url',
                'parents',
                'genders',

                'video_lists',
                'interest_lists',
                'topic_lists',
                'keyword_lists',
                'channel_lists',
            }
        )

