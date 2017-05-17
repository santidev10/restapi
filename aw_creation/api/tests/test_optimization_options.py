from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase


class AccountListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        url = reverse(
            "aw_creation_urls:optimization_options")
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
                'video_networks',
                'ad_schedule_rules',
                'location_rules',
                'devices',
                'delivery_method',
                'goal_type',
                'end',
                'type',
                'max_rate',
                'bidding_type',
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
            }
        )

