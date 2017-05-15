from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model
from datetime import datetime, timedelta
from aw_creation.models import *
from aw_reporting.models import *


class AccountListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_post(self):
        url = reverse("aw_creation_urls:creation_account")
        english, _ = Language.objects.get_or_create(id=1000,
                                                    name="English")
        geo_target = GeoTarget.objects.create(
            id=0, name="Hell", canonical_name="Hell", country_code="RU",
            target_type="place", status="hot",
        )
        start = datetime.now().date()
        end = start + timedelta(days=2)
        data = dict(
            video_ad_format=AccountCreation.IN_STREAM_TYPE,
            campaign_count=2,
            ad_group_count=3,
            video_url="https://www.youtube.com/watch?v=zaa0r2WbmYo",
            ct_overlay_text="lal234",
            final_url="https://www.channelfactory.com/",
            display_url="www.channelfactory.com",
            genders=[AdGroupCreation.GENDER_FEMALE,
                     AdGroupCreation.GENDER_MALE],
            parents=[AdGroupCreation.PARENT_PARENT,
                     AdGroupCreation.PARENT_UNDETERMINED],
            age_ranges=[AdGroupCreation.AGE_RANGE_55_64,
                        AdGroupCreation.AGE_RANGE_65_UP],
            languages=[english.id],
            location_rules=[
                dict(geo_target=geo_target.id, radius=666),
                dict(latitude=100, longitude=200, radius=2),
            ],
            devices=['DESKTOP_DEVICE'],
            frequency_capping=[
                dict(event_type=FrequencyCap.IMPRESSION_TYPE, limit=15),
                dict(event_type=FrequencyCap.VIDEO_VIEW_TYPE, limit=5),
            ],
            start=str(start), end=str(end),
            goal_type=AccountCreation.GOAL_IMPRESSIONS,
            goal_units=1000,
            budget=1000,
            max_rate="0.5",
            channel_lists=[],
            video_lists=[],
            keyword_lists=[],
            topic_lists=[],
            interest_lists=[],
        )
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'id',
                'name',
                'video_ad_format',
                'ad_group_count',
                'campaign_count',

                'video_url',
                'ct_overlay_text',
                'display_url',
                'final_url',
                'age_ranges',
                'parents',
                'genders',

                'languages',
                'location_rules',

                'devices',
                'frequency_capping',
                'start', 'end',

                'goal_units', 'goal_type',

                'budget',
                'max_rate',
            }
        )

