from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT

from aw_creation.models import *
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import *
from utils.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class CampaignAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def create_campaign(self, owner, start=None, end=None):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=start, end=end,
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

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_campaign(**defaults)
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(ac.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_get(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_campaign(**defaults)
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(ac.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def perform_format_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name', 'updated_at', 'start', 'end',
                'budget', 'languages',
                'devices', 'frequency_capping', 'ad_schedule_rules',
                'location_rules',
                'delivery_method', 'type', 'video_networks',
                'content_exclusions', 'ad_group_creations',
            }
        )
        ad_group_data = data['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_data.keys()),
            {
                'id', 'name', 'updated_at', 'ad_creations',
                'genders', 'parents', 'age_ranges',
                'targeting', 'max_rate', 'video_ad_format',
            }
        )
        self.assertEqual(
            set(ad_group_data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )

    def test_success_get_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]

        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def test_fail_update_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]

        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign.id,))

        response = self.client.patch(
            url, json.dumps({}), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_update(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        campaign = self.create_campaign(**defaults)
        account_creation = campaign.account_creation
        account_creation.is_deleted = True
        account_creation.save()

        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign.id,))

        content_exclusions = [CampaignCreation.CONTENT_LABELS[1][0]]
        request_data = dict(
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
            content_exclusions=content_exclusions,
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, False)

        self.assertEqual(len(response.data['ad_schedule_rules']), 2)
        self.assertEqual(len(response.data['frequency_capping']), 2)
        self.assertEqual(len(response.data['location_rules']), 2)
        self.assertEqual(len(response.data['devices']), 1)
        self.assertEqual(len(response.data['content_exclusions']), 1)
        self.assertEqual(response.data['content_exclusions'][0]['id'],
                         content_exclusions[0])

    def test_success_update_empty_dates(self):
        campaign = self.create_campaign(owner=self.user)
        account_creation = campaign.account_creation
        AccountCreation.objects.filter(id=account_creation.id).update(
            is_deleted=True)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, True)

        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign.id,))

        request_data = {
            "name": "Campaign 1",
            "budget": 12,
            "devices": ["DESKTOP_DEVICE", "MOBILE_DEVICE", "TABLET_DEVICE"],
            "start": None, "end": None,
            "frequency_capping": [],
            "location_rules": [],
            "languages": [1000],
            "ad_schedule_rules": [],
            "age_ranges": ["AGE_RANGE_18_24", "AGE_RANGE_25_34",
                           "AGE_RANGE_35_44", "AGE_RANGE_45_54",
                           "AGE_RANGE_55_64", "AGE_RANGE_65_UP",
                           "AGE_RANGE_UNDETERMINED"],
            "content_exclusions": ["VIDEO_RATING_DV_MA",
                                   "VIDEO_NOT_YET_RATED"],
            "parents": ["PARENT_PARENT", "PARENT_NOT_A_PARENT",
                        "PARENT_UNDETERMINED"],
            "genders": ["GENDER_FEMALE", "GENDER_MALE", "GENDER_UNDETERMINED"],
            "video_networks": ["YOUTUBE_SEARCH", "YOUTUBE_VIDEO",
                               "VIDEO_PARTNER_ON_THE_DISPLAY_NETWORK"],
        }
        response = self.client.put(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, False)

    def test_fail_set_wrong_order_dates(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        today = datetime.now().date()
        request_data = dict(
            start=str(today + timedelta(days=1)),
            end=str(today),
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST,
                         "End date must be > start date")

    def test_fail_set_start_in_the_past(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        today = datetime.now().date()
        request_data = dict(
            start=str(today - timedelta(days=2)),
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST,
                         "dates in the past are not allowed")

    def test_success_full_update_start_and_end_in_the_past(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = timezone.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="11", account_creation=account_creation,
            start=today - timedelta(days=3),
            end=today - timedelta(days=2),
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        campaign_creation.refresh_from_db()
        response = self.client.put(
            url, json.dumps(dict(
                name=campaign_creation.name,
                start=str(campaign_creation.start),
                end=str(campaign_creation.end),
                content_exclusions=campaign_creation.content_exclusions,
                devices=campaign_creation.devices,
                video_networks=campaign_creation.video_networks,
            )), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail_set_end_in_the_past(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        today = datetime.now().date()
        request_data = dict(
            end=str(today - timedelta(days=2)),
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST,
                         "dates in the past are not allowed")

    def test_set_end_when_both_dates_not_valid(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = datetime.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today - timedelta(days=10),
            end=today - timedelta(days=2),
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        request_data = dict(
            end=str(today + timedelta(days=1)),
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail_set_start_when_both_dates_not_valid(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        today = datetime.now().date()
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            start=today - timedelta(days=10),
            end=today - timedelta(days=2),
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        request_data = dict(
            start=str(today + timedelta(days=1)),
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST,
                         "Because start date > end date")

    def test_fail_delete_the_only_campaign(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_delete(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="1", account_creation=account_creation,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="2", account_creation=account_creation,
        )
        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign_creation.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

        campaign_creation.refresh_from_db()
        self.assertIs(campaign_creation.is_deleted, True)

    def test_enterprise_user_should_be_able_to_edit_campaign_creation(self):
        user = self.user
        self.fill_all_groups(user)
        campaign = self.create_campaign(owner=self.user)
        update_data = {
            "name": "Campaign 12",
            "devices": ["DESKTOP_DEVICE", "MOBILE_DEVICE", "TABLET_DEVICE"],
            "content_exclusions": ["VIDEO_RATING_DV_MA",
                                   "VIDEO_NOT_YET_RATED"],
            "video_networks": ["YOUTUBE_SEARCH", "YOUTUBE_VIDEO",
                               "VIDEO_PARTNER_ON_THE_DISPLAY_NETWORK"]
        }

        url = reverse("aw_creation_urls:campaign_creation_setup",
                      args=(campaign.id,))

        response = self.client.put(url, json.dumps(update_data),
                                   content_type='application/json')

        self.assertEqual(response.status_code, HTTP_200_OK)
