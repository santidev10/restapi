import json
from datetime import timedelta

from django.http import QueryDict
from django.utils import timezone

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import CampaignCreation
from aw_creation.models import AdGroupCreation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountBreakoutTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_BREAKOUT,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        url = f"{self._get_url(account.account_creation.id)}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_not_visible_account(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_get_success(self):
        """ Test the get method returns the expected settings data """
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        op = Opportunity.objects.create()
        pl_1 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name=f"pl_{next(int_iterator)}", opportunity=op, goal_type_id=SalesForceGoalType.CPM)
        pl_2 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name=f"pl_{next(int_iterator)}", opportunity=op, goal_type_id=SalesForceGoalType.CPV)
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, salesforce_placement=pl_1, budget=12.1, type="video")
        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, salesforce_placement=pl_2, budget=30.4, type="display")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=5000000)
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpv_bid=1000000)
        query_prams = QueryDict(f"ad_group_ids={ad_group_1.id},{ad_group_2.id}").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        data.sort(key=lambda x: x["name"])
        self.assertEqual(data[0]["name"], campaign_1.name)
        self.assertEqual(data[0]["type"], campaign_1.type)
        self.assertEqual(data[0]["budget"], campaign_1.budget)
        self.assertEqual(data[0]["max_bid"], ad_group_1.cpm_bid / 1000000)

        self.assertEqual(data[1]["name"], campaign_2.name)
        self.assertEqual(data[1]["type"], campaign_2.type)
        self.assertEqual(data[1]["budget"], campaign_2.budget)
        self.assertEqual(data[1]["max_bid"], ad_group_2.cpv_bid / 1000000)

    def test_create_fail_mixed_ad_group_types(self):
        """ Should not be able to create breakout with mixed AdGroup types """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=12.1, type="video")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=5, type="Standard")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=9.2, type="video")
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpv_bid=1, type="Display")
        payload = {
            "ad_group_ids": [
                ad_group_1.id,
                ad_group_2.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": None,
            "name": "Test Breakout - BR",
            "budget": 5,
            "max_rate": 5,
            "start": "2020-01-01",
            "end": "2020-02-02"
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_fail_start_date_less_today(self):
        """ Start date should be >= todays date """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=12.1, type="video")
        ad_group = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign, cpm_bid=5, type="Standard")
        today = timezone.now().date()
        start = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")

        payload = {
            "ad_group_ids": [
                ad_group.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": None,
            "name": "Test Breakout - BR",
            "budget": 5,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_fail_start_date_greater_end_date(self):
        """ Start date should be less than end date """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=12.1, type="video")
        ad_group = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign, cpm_bid=5, type="Standard")
        payload = {
            "ad_group_ids": [
                ad_group.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": None,
            "name": "Test Breakout - BR",
            "budget": 5,
            "max_rate": 5,
            "start": "2020-01-02",
            "end": "2020-01-01"
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_success_non_skip(self):
        """ Success create non skip breakout with updating campaign budgets """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=7.2, type="video")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=4, type="Standard")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=5.5, type="video")
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpm_bid=1, type="Standard")

        campaign_3 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=4.5, type="video")
        today = timezone.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "ad_group_ids": [
                ad_group_1.id,
                ad_group_2.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": 7,
            "name": "Test Breakout - BR",
            "budget": 5,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout = CampaignCreation.objects.get(name__contains=payload["name"])
        self.assertEqual(campaign_breakout.bid_strategy_type, CampaignCreation.TARGET_CPM_STRATEGY)
        self.assertEqual(campaign_breakout.type, CampaignCreation.VIDEO_TYPE)
        self.assertEqual(campaign_breakout.sub_type, "Non-skippable")
        self.assertEqual(campaign_breakout.budget, payload["budget"])
        self.assertEqual(str(campaign_breakout.start), payload["start"])
        self.assertEqual(str(campaign_breakout.end), payload["end"])

        # Assert related campaign creations exists for updated_campaign_budget
        campaign_1_creation = campaign_1.campaign_creation.first()
        campaign_2_creation = campaign_1.campaign_creation.first()
        self.assertEqual(campaign_1_creation.budget, payload["updated_campaign_budget"])
        self.assertEqual(campaign_2_creation.budget, payload["updated_campaign_budget"])

        ag_creation_1 = ad_group_1.ad_group_creation.first()
        ag_creation_2 = ad_group_2.ad_group_creation.first()
        self.assertEqual(ag_creation_1.max_rate, payload["max_rate"])
        self.assertEqual(ag_creation_2.max_rate, payload["max_rate"])

    def test_create_success_display(self):
        """ Success create non skip breakout with pausing source ad groups """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=8.6, type="display")
        ad_group_1a = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=3, type="Display")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=3.4, type="display")
        ad_group_2a = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpm_bid=1, type="Display")
        today = timezone.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "ad_group_ids": [
                ad_group_1a.id,
                ad_group_2a.id,
            ],
            "pause_source_ad_groups": True,
            "updated_campaign_budget": None,
            "name": "Test Display Breakout - BR",
            "budget": 3,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout = CampaignCreation.objects.get(name__contains=payload["name"])
        self.assertEqual(campaign_breakout.bid_strategy_type, CampaignCreation.MAX_CPM_STRATEGY)
        self.assertEqual(campaign_breakout.type, CampaignCreation.DISPLAY_TYPE)
        self.assertEqual(campaign_breakout.sub_type, None)
        self.assertEqual(campaign_breakout.budget, payload["budget"])
        self.assertEqual(str(campaign_breakout.start), payload["start"])
        self.assertEqual(str(campaign_breakout.end), payload["end"])

        breakout_ag_1a = AdGroupCreation.objects.get(campaign_creation=campaign_breakout, name__icontains=ad_group_1a.name)
        breakout_ag_2a = AdGroupCreation.objects.get(campaign_creation=campaign_breakout, name__icontains=ad_group_2a.name)

        self.assertEqual(breakout_ag_1a.max_rate, payload["max_rate"])
        self.assertEqual(breakout_ag_2a.max_rate, payload["max_rate"])

        ad_group_1a.refresh_from_db()
        ag_creation_1a = ad_group_1a.ad_group_creation.first()
        ag_creation_2a = ad_group_2a.ad_group_creation.first()
        self.assertEqual(ag_creation_1a.status, 0)
        self.assertEqual(ag_creation_2a.status, 0)

    def test_create_success_bumper(self):
        """ Success create bumper breakout with updating campaign budgets and pausing source ad groups """
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=7.6, type="video")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=2, type="Bumper")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=16.4, type="video")
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpm_bid=1, type="Bumper")

        campaign_3 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=4.5, type="video")
        campaign_4 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=3.3, type="video")
        today = timezone.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "ad_group_ids": [
                ad_group_1.id,
                ad_group_2.id,
            ],
            "pause_source_ad_groups": True,
            "updated_campaign_budget": 9,
            "name": "Test Bumper Breakout - BR",
            "budget": 3,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout = CampaignCreation.objects.get(name__contains=payload["name"])
        self.assertEqual(campaign_breakout.bid_strategy_type, CampaignCreation.TARGET_CPM_STRATEGY)
        self.assertEqual(campaign_breakout.type, CampaignCreation.VIDEO_TYPE)
        self.assertEqual(campaign_breakout.sub_type, None)
        self.assertEqual(campaign_breakout.budget, payload["budget"])
        self.assertEqual(str(campaign_breakout.start), payload["start"])
        self.assertEqual(str(campaign_breakout.end), payload["end"])

        ag_creation_1 = ad_group_1.ad_group_creation.first()
        ag_creation_2 = ad_group_2.ad_group_creation.first()
        self.assertEqual(ag_creation_1.status, 0)
        self.assertEqual(ag_creation_2.status, 0)

        campaign_1 = campaign_1.campaign_creation.first()
        campaign_2 = campaign_2.campaign_creation.first()
        self.assertEqual(campaign_1.budget, payload["updated_campaign_budget"])
        self.assertEqual(campaign_2.budget, payload["updated_campaign_budget"])

    def test_create_success_discovery(self):
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=4.9, type="video")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpv_bid=8, type="Video discovery")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=5.7, type="video")
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpv_bid=4, type="Video discovery")
        today = timezone.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "ad_group_ids": [
                ad_group_1.id,
                ad_group_2.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": None,
            "name": "Test Video discovery Breakout - BR",
            "budget": 3,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout = CampaignCreation.objects.get(name__contains=payload["name"])
        self.assertEqual(campaign_breakout.bid_strategy_type, CampaignCreation.MAX_CPV_STRATEGY)
        self.assertEqual(campaign_breakout.type, CampaignCreation.VIDEO_TYPE)
        self.assertEqual(campaign_breakout.sub_type, None)
        self.assertEqual(campaign_breakout.budget, payload["budget"])
        self.assertEqual(str(campaign_breakout.start), payload["start"])
        self.assertEqual(str(campaign_breakout.end), payload["end"])
        # Assert related campaign creations do not exist since updated_campaign_budget is None
        self.assertFalse(campaign_1.campaign_creation.all().exists())
        self.assertFalse(campaign_2.campaign_creation.all().exists())
        # Assert related ad group creations do not exist since updated_campaign_budget is None

        ag_creation_1 = ad_group_1.ad_group_creation.first()
        ag_creation_2 = ad_group_2.ad_group_creation.first()
        self.assertEqual(ag_creation_1.max_rate, payload["max_rate"])
        self.assertEqual(ag_creation_2.max_rate, payload["max_rate"])

    def test_create_success_instream(self):
        self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=1.23, type="video")
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=4, type="In-stream")

        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, budget=1.7, type="video")
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpm_bid=1, type="In-stream")
        today = timezone.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "ad_group_ids": [
                ad_group_1.id,
                ad_group_2.id,
            ],
            "pause_source_ad_groups": False,
            "updated_campaign_budget": None,
            "name": "Test Video In-stream Breakout - BR",
            "budget": 3,
            "max_rate": 5,
            "start": start,
            "end": end,
        }
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self._get_url(account.account_creation.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout = CampaignCreation.objects.get(name__contains=payload["name"])
        self.assertEqual(campaign_breakout.bid_strategy_type, CampaignCreation.MAX_CPV_STRATEGY)
        self.assertEqual(campaign_breakout.type, CampaignCreation.VIDEO_TYPE)
        self.assertEqual(campaign_breakout.sub_type, None)
        self.assertEqual(campaign_breakout.budget, payload["budget"])
        self.assertEqual(str(campaign_breakout.start), payload["start"])
        self.assertEqual(str(campaign_breakout.end), payload["end"])
        # Assert related campaign creations do not exist since updated_campaign_budget is None
        self.assertFalse(campaign_1.campaign_creation.all().exists())
        self.assertFalse(campaign_2.campaign_creation.all().exists())
        # Assert related ad group creations do not exist since updated_campaign_budget is None

        ag_creation_1 = ad_group_1.ad_group_creation.first()
        ag_creation_2 = ad_group_2.ad_group_creation.first()
        self.assertEqual(ag_creation_1.max_rate, payload["max_rate"])
        self.assertEqual(ag_creation_2.max_rate, payload["max_rate"])

    def test_update_campaign_budget(self):
        """ Should have entries to update source campaign budgets """
        pass