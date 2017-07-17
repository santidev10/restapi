from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_creation.models import *
from aw_reporting.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def create_account_creation(owner, start, end):
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
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
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
        return account_creation

    def test_success_post(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = reverse("aw_creation_urls:account_creation_duplicate",
                      args=(ac.id,))

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertNotEqual(ac.id, data['id'])
        self.perform_details_check(data)
        self.assertEqual(data['name'], "Pep (copy)")

    def test_success_post_demo(self):
        url = reverse("aw_creation_urls:account_creation_duplicate",
                      args=(DEMO_ACCOUNT_ID,))
        with patch(
            "aw_creation.api.serializers.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertNotEqual(DEMO_ACCOUNT_ID, data['id'])
        self.perform_details_check(data)
        self.assertEqual(data['name'], "Demo (copy)")

    def perform_details_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name', 'updated_at',
                'is_paused', 'is_ended', 'is_approved', 'updated_at',
                'campaign_creations',
            }
        )

        campaign_data = data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_data.keys()),
            {
                'id', 'name', 'updated_at',
                'start', 'end',
                'budget', 'languages',
                'devices', 'frequency_capping', 'ad_schedule_rules',
                'location_rules', 'ad_group_creations',
                "video_ad_format", "delivery_method", "video_networks",
                'content_exclusions', 'genders', 'age_ranges', 'parents',
            }
        )
        self.assertEqual(
            campaign_data['video_ad_format'],
            dict(id=CampaignCreation.IN_STREAM_TYPE,
                 name=CampaignCreation.VIDEO_AD_FORMATS[0][1]),
        )
        self.assertEqual(
            campaign_data['delivery_method'],
            dict(id=CampaignCreation.STANDARD_DELIVERY,
                 name=CampaignCreation.DELIVERY_METHODS[0][1]),
        )
        self.assertEqual(
            campaign_data['video_networks'],
            [dict(id=uid, name=n)
             for uid, n in CampaignCreation.VIDEO_NETWORKS],
        )
        self.assertEqual(len(campaign_data['languages']), 1)
        self.assertEqual(
            campaign_data['languages'][0],
            dict(id=1000, name="English"),
        )
        self.assertEqual(len(campaign_data['location_rules']), 1)
        self.assertEqual(
            set(campaign_data['location_rules'][0].keys()),
            {
                'longitude',
                'radius',
                'latitude',
                'bid_modifier',
                'radius_units',
                'geo_target',
            }
        )
        self.assertEqual(len(campaign_data['devices']), 3)
        self.assertEqual(
            set(campaign_data['devices'][0].keys()),
            {'id', 'name'},
        )
        self.assertEqual(
            set(campaign_data['location_rules'][0]['radius_units']),
            {'id', 'name'}
        )
        self.assertEqual(len(campaign_data['frequency_capping']), 1)
        self.assertEqual(
            set(campaign_data['frequency_capping'][0].keys()),
            {
                'event_type',
                'limit',
                'level',
                'time_unit',
            }
        )
        for f in ('event_type', 'level', 'time_unit'):
            self.assertEqual(
                set(campaign_data['frequency_capping'][0][f].keys()),
                {'id', 'name'}
            )

        self.assertGreaterEqual(len(campaign_data['ad_schedule_rules']), 1)
        self.assertEqual(
            set(campaign_data['ad_schedule_rules'][0].keys()),
            {
                'from_hour',
                'from_minute',
                'campaign_creation',
                'to_minute',
                'to_hour',
                'day',
            }
        )
        ad_group_data = campaign_data['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_data.keys()),
            {
                'id', 'updated_at', 'name', 'ad_creations',
                'genders', 'parents', 'age_ranges', 'max_rate',
                # targeting
                'targeting',
            }
        )
        self.assertEqual(
            set(ad_group_data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )
        self.assertEqual(
            set(ad_group_data['targeting']['keyword'][0]),
            {'criteria', 'is_negative', 'type', 'name'}
        )

        ad = ad_group_data['ad_creations'][0]
        self.assertEqual(
            set(ad.keys()),
            {
                'id', 'updated_at', 'custom_params', 'name', 'tracking_template',
                'video_url', 'display_url', 'final_url', 'thumbnail',
            }
        )
