from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import GeoTarget
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse


class AccountAPITestCase(AwReportingAPITestCase):

    def _get_url(self, campaign_id):
        return reverse(Name.CreationSetup.CAMPAIGN_DUPLICATE, [Namespace.AW_CREATION], args=(campaign_id,))

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    @staticmethod
    def create_campaign_creation(owner):
        account_creation = AccountCreation.objects.create(
            name="", owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
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
        AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
            is_deleted=True,
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
        AdCreation.objects.create(
            name="",
            ad_group_creation=ad_group_creation,
            is_deleted=True,
        )
        return campaign_creation

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        campaign = self.create_campaign_creation(self.user)
        url = self._get_url(campaign.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_post(self):
        campaign = self.create_campaign_creation(self.user)
        url = self._get_url(campaign.id)

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_data = response.data
        self.assertNotEqual(campaign.id, campaign_data['id'])

        self.assertEqual(
            set(campaign_data.keys()),
            {
                "ad_group_creations",
                "ad_schedule_rules",
                "budget",
                "content_exclusions",
                "delivery_method",
                "devices",
                "end",
                "frequency_capping",
                "id",
                "is_draft",
                "languages",
                "location_rules",
                "name",
                "start",
                "type",
                "updated_at",
                "video_networks",
                "bid_strategy_type",
                "sync_at",
                "target_cpa"
            }
        )
        self.assertEqual(campaign_data['name'], "{} (1)".format(campaign.name))
        self.assertEqual(
            campaign_data['type'],
            dict(id=CampaignCreation.CAMPAIGN_TYPES[0][0],
                 name=CampaignCreation.CAMPAIGN_TYPES[0][1]),
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
                'id',
                'from_hour',
                'from_minute',
                'campaign_creation',
                'to_minute',
                'to_hour',
                'day',
            }
        )
        self.assertEqual(len(campaign_data['ad_group_creations']), 1)

    def test_success_post_increment_name(self):
        account_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        campaign = CampaignCreation.objects.create(
            name="FF 1 (665)", account_creation=account_creation,
        )
        url = self._get_url(campaign.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data['name'], "FF 1 (666)")

    def test_success_post_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        url = self._get_url(campaign.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
