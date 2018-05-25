from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from utils.utils_tests import ExtendedAPITestCase


class AccountListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        url = reverse("aw_creation_urls:creation_options")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'name',
                'start', 'end',
                'video_ad_format',

                'budget',
                'video_networks',
                'ad_schedule_rules',
                'location_rules',
                'devices',
                'delivery_method',
                'goal_type',

                'type',
                'max_rate',
                'bidding_type',
                'languages',
                'goal_units',
                'frequency_capping',

                'ct_overlay_text',
                'video_url',
                'final_url',
                'age_ranges',
                'display_url',
                'parents',
                'genders',
                'content_exclusions',
            }
        )

        self.assertEqual(len(response.data['video_ad_format']), 3)
        self.assertEqual(set(response.data['video_ad_format'][0].keys()), {"id", "name", "thumbnail"})

