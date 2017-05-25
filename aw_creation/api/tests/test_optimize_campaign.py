from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST

from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase


class CampaignAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_campaign(self, owner, start, end):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="",
            account_creation=account_creation,
            start=start,
            end=end,
        )
        english, _ = Language.objects.get_or_create(id=1000,
                                                    name="English")
        campaign_creation.languages.add(english)

        # location rule
        geo_target = GeoTarget.objects.create(
            id=0, name="Hell", canonical_name="Hell", country_code="RU",
            target_type="place", status="hot",
        )
        LocationRule.objects.create(
            campaign_creation=campaign_creation,
            geo_target=geo_target,
        )
        FrequencyCap.objects.create(
            campaign_creation=campaign_creation,
            limit=10,
        )
        AdScheduleRule.objects.create(
            campaign_creation=campaign_creation,
            day=1,
            from_hour=6,
            to_hour=18,
        )
        AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        return campaign_creation

    def test_success_get(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_campaign(**defaults)
        url = reverse("aw_creation_urls:optimization_campaign",
                      args=(ac.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
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
        ad_group_data = data['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_data.keys()),
            {
                'id', 'name', 'thumbnail', 'is_approved',
                'video_url', 'ct_overlay_text', 'display_url', 'final_url',
                'max_rate',
                'genders', 'parents', 'age_ranges',
                # targeting
                'targeting',
            }
        )
        for f in ('age_ranges', 'genders', 'parents'):
            self.assertGreater(len(ad_group_data[f]), 1)
            self.assertEqual(
                set(ad_group_data[f][0].keys()),
                {'id', 'name'}
            )
        self.assertEqual(
            set(ad_group_data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )

    def test_success_update(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        campaign = self.create_campaign(**defaults)
        url = reverse("aw_creation_urls:optimization_campaign",
                      args=(campaign.id,))

        request_data = dict(
            is_paused=True,
            ad_schedule_rules=[
                dict(day=1, from_hour=6, to_hour=18),
                dict(day=2, from_hour=6, to_hour=18),
            ],
            frequency_capping=[
                dict(event_type=FrequencyCap.IMPRESSION_TYPE, limit=15),
                dict(event_type=FrequencyCap.VIDEO_VIEW_TYPE, limit=5),
            ],
            location_rules=[
                dict(geo_target=0, radius=666),
                dict(latitude=100, longitude=200, radius=2),
            ],
            devices=['DESKTOP_DEVICE'],
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['is_paused'], True)
        self.assertEqual(len(response.data['ad_schedule_rules']), 2)
        self.assertEqual(len(response.data['frequency_capping']), 2)
        self.assertEqual(len(response.data['location_rules']), 2)
        self.assertEqual(len(response.data['devices']), 1)

    def test_fail_approve(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:optimization_campaign",
                      args=(campaign_creation.id,))

        request_data = dict(is_approved=True)
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data['non_field_errors'][0],
            "These fields are required for approving: "
            "start date, end date, budget, max rate, goal"
        )

    def test_success_approve(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = datetime.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today, end=today + timedelta(days=1),
            budget="20.5", max_rate="2.2", goal_units=1000,
        )
        url = reverse("aw_creation_urls:optimization_campaign",
                      args=(campaign_creation.id,))

        request_data = dict(is_approved=True)
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)



