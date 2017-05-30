from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_202_ACCEPTED

from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase


class PostAccountAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_post(self):
        url = reverse("aw_creation_urls:optimization_account_list")
        data = dict(
            name="Test account",
            video_ad_format=AccountCreation.IN_STREAM_TYPE,
            campaign_count=3,
            ad_group_count=5,
        )
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

        self.assertEqual(
            set(response.data.keys()),
            {
                # common details
                'id', 'name', 'status',
                'is_ended', 'is_approved', 'is_paused', 'is_changed',
                'is_optimization_active', "campaign_creations",

                'ordered_cpv',
                'cpv',
                'ordered_impressions_cost',
                'ordered_views_cost',
                'impressions',
                'views',
                'ordered_views',
                'impressions_cost',
                'cpm',
                'ordered_cpm',
                'ordered_impressions',
                'views_cost',

                # details below header
                "goal_type", "type", "video_ad_format", "delivery_method",
                "video_networks", "bidding_type",
                # details below header (readonly)
                "budget", 'start', 'end',
            }
        )

        campaign_creation = response.data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_creation.keys()),
            {
                'id', 'name',
                'is_approved', 'is_paused',
                'start', 'end',
                'goal_units', 'budget', 'max_rate', 'languages',
                'devices', 'frequency_capping', 'ad_schedule_rules',
                'location_rules',
                'ad_group_creations',
            }
        )

        ad_group_creation = campaign_creation['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {
                'id', 'name', 'thumbnail', 'is_approved',
                'video_url', 'ct_overlay_text', 'display_url', 'final_url',
                'max_rate',
                'genders', 'parents', 'age_ranges',
                'targeting',
            }
        )

        self.assertEqual(
            set(ad_group_creation['targeting'].keys()),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )

