from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import *
from userprofile.constants import StaticPermissions


class AccountAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.MEDIA_BUYING: True,
        })

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

    def perform_details_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                "id", "name", "account", "updated_at",
                "is_paused", "is_ended", "is_approved", "updated_at",
                "campaign_creations",
            }
        )

        campaign_data = data["campaign_creations"][0]
        self.assertEqual(
            set(campaign_data.keys()),
            {
                "id", "name", "updated_at",
                "start", "end",
                "budget", "languages",
                "devices", "frequency_capping", "ad_schedule_rules",
                "location_rules", "ad_group_creations",
                "type", "delivery_method", "video_networks",
                "content_exclusions",
            }
        )
        self.assertEqual(
            campaign_data["type"],
            dict(id=CampaignCreation.CAMPAIGN_TYPES[0][0],
                 name=CampaignCreation.CAMPAIGN_TYPES[0][1]),
        )
        self.assertEqual(
            campaign_data["delivery_method"],
            dict(id=CampaignCreation.STANDARD_DELIVERY,
                 name=CampaignCreation.DELIVERY_METHODS[0][1]),
        )
        self.assertEqual(
            campaign_data["video_networks"],
            [dict(id=uid, name=n)
             for uid, n in CampaignCreation.VIDEO_NETWORKS],
        )
        self.assertEqual(len(campaign_data["languages"]), 1)
        self.assertEqual(
            campaign_data["languages"][0],
            dict(id=1000, name="English"),
        )
        self.assertEqual(len(campaign_data["location_rules"]), 1)
        self.assertEqual(
            set(campaign_data["location_rules"][0].keys()),
            {
                "longitude",
                "radius",
                "latitude",
                "bid_modifier",
                "radius_units",
                "geo_target",
            }
        )
        self.assertEqual(len(campaign_data["devices"]), 3)
        self.assertEqual(
            set(campaign_data["devices"][0].keys()),
            {"id", "name"},
        )
        self.assertEqual(
            set(campaign_data["location_rules"][0]["radius_units"]),
            {"id", "name"}
        )
        self.assertEqual(len(campaign_data["frequency_capping"]), 1)
        self.assertEqual(
            set(campaign_data["frequency_capping"][0].keys()),
            {
                "event_type",
                "limit",
                "level",
                "time_unit",
            }
        )
        for f in ("event_type", "level", "time_unit"):
            self.assertEqual(
                set(campaign_data["frequency_capping"][0][f].keys()),
                {"id", "name"}
            )

        self.assertGreaterEqual(len(campaign_data["ad_schedule_rules"]), 1)
        self.assertEqual(
            set(campaign_data["ad_schedule_rules"][0].keys()),
            {
                "from_hour",
                "from_minute",
                "campaign_creation",
                "to_minute",
                "to_hour",
                "day",
            }
        )
        ad_group_data = campaign_data["ad_group_creations"][0]
        self.assertEqual(
            set(ad_group_data.keys()),
            {
                "id", "updated_at", "name", "ad_creations",
                "genders", "parents", "age_ranges", "max_rate",
                "video_ad_format",
                # targeting
                "targeting",
            }
        )
        self.assertEqual(
            set(ad_group_data["targeting"]),
            {"channel", "video", "topic", "interest", "keyword"}
        )
        self.assertEqual(
            set(ad_group_data["targeting"]["keyword"]["negative"][0]),
            {"criteria", "is_negative", "type", "name"}
        )

        self.assertEqual(len(ad_group_data["ad_creations"]), 1)
