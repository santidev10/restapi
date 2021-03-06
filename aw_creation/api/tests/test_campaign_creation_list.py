import json
from datetime import timedelta

from django.db import connections
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN

from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import Language
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.test_case import ExtendedAPITestCase


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
        self.user = self.create_test_user(perms={
            StaticPermissions.MEDIA_BUYING: True,
        })

    def test_success_fail_has_no_permission(self):
        self.user.perms.update({
            StaticPermissions.MEDIA_BUYING: False,
        })
        self.user.save()

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
        self.perform_get_format_check(response.data)

    def test_success_get_demo(self):
        recreate_test_demo_data()
        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(DEMO_ACCOUNT_ID,))
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def perform_get_format_check(self, data):
        campaign_keys = {
            "ad_group_creations",
            "ad_schedule_rules",
            "bid_strategy_type",
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
            "sync_at",
            "target_cpa",
            "type",
            "updated_at",
            "video_networks",
        }
        self.assertEqual(len(data), 2)
        self.assertEqual(
            set(data[0].keys()),
            campaign_keys
        )

    def test_success_post(self):
        detail_keys = set(self.detail_keys)
        detail_keys.add("sync_at")
        detail_keys.add("target_cpa")
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
        self.assertEqual(len(response.data["languages"]), 1)

    def test_fail_post_demo(self):
        url = reverse("aw_creation_urls:campaign_creation_list_setup",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.post(
            url, json.dumps(dict()), content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
