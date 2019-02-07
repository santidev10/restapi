import json
from datetime import timedelta
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import AccountCreation, CampaignCreation, Language
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from utils.datetime import now_in_default_tz
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase


class CampaignListAPITestCase(ExtendedAPITestCase):
    detail_keys = {
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
    }

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_get(self):
        today = now_in_default_tz().date()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="1", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )
        CampaignCreation.objects.create(
            name="2", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )
        CampaignCreation.objects.create(
            name="3", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
            is_deleted=True,
        )

        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data, extra_data_keys=['sync_at'])

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(DEMO_ACCOUNT_ID,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def perform_get_format_check(self, data, extra_data_keys=None):
        campaign_keys = {
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
        }
        if extra_data_keys is not None:
            campaign_keys.update(extra_data_keys)
        self.assertEqual(len(data), 2)
        self.assertEqual(
            set(data[0].keys()),
            campaign_keys
        )

    def test_success_post(self):
        detail_keys = set(self.detail_keys)
        detail_keys.add('sync_at')
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )

        for lid in (1000, 1003):
            Language.objects.get_or_create(id=lid, defaults=dict(name=""))

        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(account_creation.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(
            set(response.data.keys()),
            detail_keys,
        )
        self.assertEqual(len(response.data['languages']), 1)

    def test_fail_post_demo(self):
        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.post(
            url, json.dumps(dict()), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
