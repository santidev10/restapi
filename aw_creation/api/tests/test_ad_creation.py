import json
from datetime import timedelta
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT

from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, AdCreation
from aw_reporting.demo.models import DemoAccount
from utils.datetime import now_in_default_tz
from utils.utittests.generic_test import generic_test
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase


class AdGroupAPITestCase(ExtendedAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def create_ad(self, owner, start=None, end=None, account=None,
                  beacon_view_1=""):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=owner, account=account,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="",
            account_creation=account_creation,
            start=start,
            end=end,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="Test Ad", ad_group_creation=ad_group_creation,
            custom_params_raw='[{"name": "test", "value": "ad"}]',
            beacon_view_1=beacon_view_1,
        )
        return ad_creation

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_get(self):
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def perform_format_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                "beacon_completed_1",
                "beacon_completed_2",
                "beacon_completed_3",
                "beacon_dcm_1",
                "beacon_dcm_2",
                "beacon_dcm_3",
                "beacon_first_quartile_1",
                "beacon_first_quartile_2",
                "beacon_first_quartile_3",
                "beacon_impression_1",
                "beacon_impression_2",
                "beacon_impression_3",
                "beacon_midpoint_1",
                "beacon_midpoint_2",
                "beacon_midpoint_3",
                "beacon_skip_1",
                "beacon_skip_2",
                "beacon_skip_3",
                "beacon_third_quartile_1",
                "beacon_third_quartile_2",
                "beacon_third_quartile_3",
                "beacon_vast_1",
                "beacon_vast_2",
                "beacon_vast_3",
                "beacon_view_1",
                "beacon_view_2",
                "beacon_view_3",
                "description_1",
                "description_2",
                "headline",
                "is_disapproved",
                "companion_banner",
                "custom_params",
                "display_url",
                "final_url",
                "id",
                "name",
                "tracking_template",
                "updated_at",
                "video_ad_format",
                "video_channel_title",
                "video_description",
                "video_duration",
                "video_id",
                "video_thumbnail",
                "video_title",
                "video_url",
                "long_headline",
                "short_headline",
                "business_name"
            }
        )
        if len(data["custom_params"]) > 0:
            self.assertEqual(
                set(data["custom_params"][0].keys()),
                {'value', 'name'}
            )

    def test_success_get_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def test_fail_update_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.patch(
            url, json.dumps({}), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_update(self):
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
            beacon_view_1="http://www.test.ua",
        )
        ad = self.create_ad(**defaults)
        campaign_creation = ad.ad_group_creation.campaign_creation
        account_creation = campaign_creation.account_creation
        account_creation.is_deleted = True
        account_creation.save()

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        with open('aw_creation/fixtures/tests/video_thumbnail.png', 'rb') as fp:
            data = dict(
                name="Ad Group 1",
                final_url="https://wtf.com",
                tracking_template="https://track.com?why",
                custom_params=json.dumps([{"name": "name1", "value": "value2"},
                                          {"name": "name2",
                                           "value": "value2"}]),
                companion_banner=fp,
                video_ad_format=AdGroupCreation.BUMPER_AD,
                beacon_first_quartile_3="http://tracking.com.kz?let_me_go=1",
                beacon_view_1="",
                beacon_view_2="",  # This field is sent but hasn't been changed
            )
            response = self.client.patch(url, data, format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, False)

        ad.refresh_from_db()
        self.assertEqual(ad.name, data['name'])
        self.assertEqual(ad.final_url, data['final_url'])
        self.assertEqual(ad.tracking_template, data['tracking_template'])
        self.assertEqual(ad.beacon_first_quartile_3,
                         data['beacon_first_quartile_3'])
        self.assertIs(ad.beacon_first_quartile_3_changed, True)
        self.assertEqual(ad.beacon_view_1, data['beacon_view_1'])
        self.assertIs(ad.beacon_view_1_changed, True)
        self.assertEqual(ad.beacon_view_2, data['beacon_view_2'])
        self.assertIs(ad.beacon_view_2_changed, False)
        self.assertEqual(ad.custom_params,
                         [{"name": "name1", "value": "value2"},
                          {"name": "name2", "value": "value2"}])
        self.assertIsNotNone(ad.companion_banner)

        ad.ad_group_creation.refresh_from_db()
        self.assertEqual(ad.ad_group_creation.video_ad_format,
                         data["video_ad_format"])

        campaign_creation = ad.ad_group_creation.campaign_creation
        campaign_creation.refresh_from_db()
        self.assertEqual(campaign_creation.bid_strategy_type,
                         CampaignCreation.MAX_CPM_STRATEGY)

    def test_success_update_json(self):
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        data = dict(
            name="Ad Group  1",
            final_url="https://wtf.com",
            tracking_template="https://track.com?why",
            custom_params=[{"name": "name1", "value": "value2"},
                           {"name": "name2", "value": "value2"}],
        )
        response = self.client.patch(url, json.dumps(data),
                                     content_type='application/json')

        self.assertEqual(response.status_code, HTTP_200_OK)
        ad.refresh_from_db()

        for f, v in data.items():
            self.assertEqual(getattr(ad, f), v)

    def test_fail_delete_the_only(self):
        ad = self.create_ad(owner=self.user)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_delete(self):
        ad = self.create_ad(owner=self.user)
        AdCreation.objects.create(
            name="",
            ad_group_creation=ad.ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        ad.refresh_from_db()
        self.assertIs(ad.is_deleted, True)

    def test_enterprise_user_can_edit_any_ad(self):
        self.fill_all_groups(self.user)
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        data = dict(
            name="Ad Group  1",
            final_url="https://wtf.com",
            tracking_template="https://track.com?why",
            custom_params=[{"name": "name1", "value": "value2"},
                           {"name": "name2", "value": "value2"}],
        )
        response = self.client.patch(url, json.dumps(data),
                                     content_type='application/json')

        self.assertEqual(response.status_code, HTTP_200_OK)
        ad.refresh_from_db()

        for f, v in data.items():
            self.assertEqual(getattr(ad, f), v)

    @generic_test([
        (None, (field,), dict())
        for field in ("headline", "description_1", "description_2")
    ])
    def test_discovery_fields_not_none(self, property_name):
        self.fill_all_groups(self.user)
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        ad.ad_group_creation.video_ad_format = AdGroupCreation.DISCOVERY_TYPE
        ad.ad_group_creation.save()
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        data = dict(
            name="Ad Group  1",
            headline="headline",
            description_1="description_1",
            description_2="description_2",
            final_url="https://wtf.com",
            tracking_template="https://track.com?why",
            custom_params=[{"name": "name1", "value": "value2"},
                           {"name": "name2", "value": "value2"}],
        )
        data[property_name] = None
        response = self.client.patch(url, json.dumps(data),
                                     content_type="application/json")

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertIn(property_name, response.data)

    def test_headline_limit(self):
        self.fill_all_groups(self.user)
        today = now_in_default_tz().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        ad.ad_group_creation.video_ad_format = AdGroupCreation.DISCOVERY_TYPE
        ad.ad_group_creation.save()
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        data = dict(
            name="Ad Group  1",
            short_headline="h" * 26,
        )
        response = self.client.patch(url, json.dumps(data),
                                     content_type="application/json")

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertIn("short_headline", response.data)
