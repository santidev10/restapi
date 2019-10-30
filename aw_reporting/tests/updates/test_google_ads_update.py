from types import SimpleNamespace

from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from django.db import Error
from django.db.backends.utils import CursorWrapper
from django.test import TransactionTestCase
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.v1.services.enums import DeviceEnum
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import RefreshError
from pytz import timezone
from pytz import utc
from unittest.mock import MagicMock
from unittest.mock import patch

from aw_creation.models import AccountCreation
from aw_reporting.google_ads.constants import MIN_FETCH_DATE
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.accounts import AccountUpdater
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.ads import AdUpdater
from aw_reporting.google_ads.updaters.age_range import AgeRangeUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.campaign_location_target import CampaignLocationTargetUpdater
from aw_reporting.google_ads.updaters.cf_account_connection import CFAccountConnector
from aw_reporting.google_ads.updaters.interests import InterestUpdater
from aw_reporting.google_ads.updaters.parents import ParentUpdater
from aw_reporting.google_ads.updaters.topics import TopicUpdater
from aw_reporting.google_ads.tasks.update_campaigns import cid_campaign_update
from aw_reporting.google_ads.tasks.update_campaigns import setup_update_campaigns
from aw_reporting.models import ALL_AGE_RANGES
from aw_reporting.models import ALL_DEVICES
from aw_reporting.models import ALL_GENDERS
from aw_reporting.models import ALL_PARENTS
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import BudgetType
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import ParentStatistic
from aw_reporting.models import ParentStatuses
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.ad_words.constants import Device
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from utils.exception import ExceptionWithArgs
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.mock_google_ads_response import MockGoogleAdsAPIResponse
from utils.utittests.patch_now import patch_now
from utils.utittests.redis_mock import MockRedis


class UpdateGoogleAdsTestCase(TransactionTestCase):
    def _create_account(self, manager_update_time=None, tz="UTC", account_update_time=None, **kwargs):
        mcc_account = Account.objects.create(id=next(int_iterator), timezone=tz,
                                             can_manage_clients=True,
                                             update_time=manager_update_time)
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=AWConnection.objects.create(),
                                           can_read=True)

        account_id = kwargs.pop("id", next(int_iterator))
        account = Account.objects.create(id=account_id, timezone=tz, update_time=account_update_time,
                                         **kwargs)
        account.managers.add(mcc_account)
        account.save()
        return account

    def setUp(self):
        self.redis_mock = patch('utils.celery.tasks.REDIS_CLIENT', MockRedis())
        self.redis_mock.start()

    def tearDown(self):
        self.redis_mock.stop()

    @staticmethod
    def create_resource_name(resource, _id):
        return f"{resource}/{_id}"

    @staticmethod
    def create_click_report_key(resource_name, date):
        return f"{resource_name}/{date}"

    def test_existing_mcc_account_doesnt_reset_defaults(self):
        mcc_account = Account.objects.create(
            id=next(int_iterator),
            name="MCC",
            can_manage_clients=True,
            currency_code="YEN"
        )
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=AWConnection.objects.create(),
                                           can_read=True)
        managed_mcc_account = Account.objects.create(
            id=next(int_iterator),
            name="Original Name",
            can_manage_clients=True,
            is_active=False,
        )
        managed_mcc_account.managers.add(mcc_account.id)
        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", managed_mcc_account.id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "Changed Name")
        mock_customer_client_data.set("customer_client", "currency_code", "USD")
        mock_customer_client_data.set("customer_client", "time_zone", "UTC")
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        client = GoogleAdsClient("", "")
        updater = AccountUpdater(mcc_account)
        updater.get_client_customer_accounts = MagicMock(return_value=mock_customer_client_data)
        updater.update(client)

        managed_mcc_account.refresh_from_db()
        self.assertTrue(managed_mcc_account.is_active is False)
        self.assertTrue(managed_mcc_account.name == "Changed Name")
        self.assertTrue(managed_mcc_account.currency_code == "USD")

    def test_update_campaign_aggregated_stats(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(
            id=1,
            account=account,
            de_norm_fields_are_recalculated=True,
            start_date=today - timedelta(days=5),
            end_date=today + timedelta(days=5),
            sync_time=datetime.now(tz=utc) + timedelta(days=5),
            cost=1,
            budget=1,
            impressions=1,
            video_views=1,
            clicks=1
        )
        costs = [2, 3]
        impressions = [4, 5]
        views = [6, 7]
        clicks = [8, 9]

        self.assertNotEqual(campaign.cost, sum(costs))
        self.assertNotEqual(campaign.impressions, sum(impressions))
        self.assertNotEqual(campaign.video_views, sum(views))
        self.assertNotEqual(campaign.clicks, sum(clicks))
        dates = (today - timedelta(days=2), today - timedelta(days=1))
        dates_len = len(dates)
        statistic = zip(dates, costs, impressions, views, clicks)

        mock_hourly_performance_response = MockGoogleAdsAPIResponse()
        mock_campaign_data = MockGoogleAdsAPIResponse()
        for dt, cost, impression, view, click in statistic:
            mock_campaign_data.set("campaign", "resource_name", self.create_resource_name("campaign", campaign.id), nested_key=None)
            mock_campaign_data.set("campaign", "id", campaign.id)
            mock_campaign_data.set("campaign", "name", "test")
            mock_campaign_data.set("campaign", "status", 2, nested_key=None)
            mock_campaign_data.set("campaign", "serving_status", 2, nested_key=None)
            mock_campaign_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
            mock_campaign_data.set("segments", "date", dt)
            mock_campaign_data.set("campaign", "start_date", str(campaign.start_date))
            mock_campaign_data.set("campaign", "end_date", str(campaign.end_date))
            mock_campaign_data.set("campaign_budget", "amount_micros", campaign.budget * 10 ** 6)
            mock_campaign_data.set("campaign_budget", "total_amount_micros", None)
            mock_campaign_data.set("metrics", "impressions", impression)
            mock_campaign_data.set("metrics", "video_views", view)
            mock_campaign_data.set("metrics", "clicks", click)
            mock_campaign_data.set("metrics", "conversions", 0)
            mock_campaign_data.set("metrics", "all_conversions", 0)
            mock_campaign_data.set("metrics", "view_through_conversions", 0)
            mock_campaign_data.set("metrics", "cost_micros", cost * 10 ** 6)
            mock_campaign_data.set("metrics", "video_quartile_25_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_50_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_75_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_100_rate", 0)
            mock_campaign_data.set("segments", "device", 0, nested_key=None)
            mock_campaign_data.add_row()

        website_clicks = 1
        call_to_action_overlay_clicks = 2
        app_store_clicks = 3
        cards_clicks = 4
        end_cap_clicks = 5
        test_click_data = {}

        for dt in dates:
            resource_name = self.create_resource_name("campaign", campaign.id)
            key = self.create_click_report_key(resource_name, dt)
            test_click_data[key] = {
                "clicks_website": website_clicks,
                "clicks_call_to_action_overlay": call_to_action_overlay_clicks,
                "clicks_cards": cards_clicks,
                "clicks_end_cap": end_cap_clicks,
                "clicks_app_store": app_store_clicks
            }

        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=(mock_campaign_data, test_click_data))
        updater._get_campaign_hourly_performance = MagicMock(return_value=mock_hourly_performance_response)

        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)
        campaign.refresh_from_db()

        self.assertEqual(campaign.cost, sum(costs))
        self.assertEqual(campaign.impressions, sum(impressions))
        self.assertEqual(campaign.video_views, sum(views))
        self.assertEqual(campaign.clicks, sum(clicks))
        self.assertEqual(campaign.clicks_website, website_clicks * dates_len)
        self.assertEqual(campaign.clicks_call_to_action_overlay, call_to_action_overlay_clicks * dates_len)
        self.assertEqual(campaign.clicks_app_store, app_store_clicks * dates_len)
        self.assertEqual(campaign.clicks_cards, cards_clicks * dates_len)
        self.assertEqual(campaign.clicks_end_cap, end_cap_clicks * dates_len)

    def test_update_ad_group_aggregated_stats(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1,
                                          campaign=campaign,
                                          de_norm_fields_are_recalculated=True,
                                          cost=1,
                                          impressions=1,
                                          video_views=1,
                                          clicks=1,
                                          engagements=1,
                                          active_view_impressions=1,
                                          name="test"
                                          )
        costs = (2, 3)
        impressions = (4, 5)
        views = (6, 7)
        clicks = (8, 9)
        engagements = (10, 11)
        active_view_impressions = (12, 13)
        website_clicks = 1
        call_to_action_overlay_clicks = 2
        app_store_clicks = 3
        cards_clicks = 4
        end_cap_clicks = 5

        self.assertNotEqual(ad_group.cost, sum(costs))
        self.assertNotEqual(ad_group.impressions, sum(impressions))
        self.assertNotEqual(ad_group.video_views, sum(views))
        self.assertNotEqual(ad_group.clicks, sum(clicks))
        self.assertNotEqual(ad_group.engagements, sum(engagements))
        self.assertNotEqual(ad_group.active_view_impressions,
                            sum(active_view_impressions))

        dates = (today - timedelta(days=2), today - timedelta(days=1))
        dates_len = len(dates)
        statistic = zip(dates, costs, impressions, views, clicks, engagements,
                        active_view_impressions)

        mock_ad_group_data = MockGoogleAdsAPIResponse()
        for dt, cost, impression, view, click, engs, avi in statistic:
            mock_ad_group_data.set("campaign", "id", campaign.id)
            mock_ad_group_data.set("ad_group", "resource_name", self.create_resource_name("ad_group", ad_group.id), nested_key=None)
            mock_ad_group_data.set("ad_group", "id", ad_group.id)
            mock_ad_group_data.set("ad_group", "name", ad_group.name)
            mock_ad_group_data.set("ad_group", "status", 2, nested_key=None) # ENABLED
            mock_ad_group_data.set("ad_group", "type", 7, nested_key=None)  # VIDEO_BUMPER
            mock_ad_group_data.set("ad_group", "cpv_bid_micros", 10000)
            mock_ad_group_data.set("ad_group", "cpm_bid_micros", None)
            mock_ad_group_data.set("ad_group", "cpc_bid_micros", None)
            mock_ad_group_data.set("metrics", "average_position", 1)
            mock_ad_group_data.set("metrics", "cost_micros", cost * 10 ** 6)
            mock_ad_group_data.set("metrics", "impressions", impression)
            mock_ad_group_data.set("metrics", "video_views", view)
            mock_ad_group_data.set("metrics", "clicks", click)
            mock_ad_group_data.set("metrics", "conversions", 0)
            mock_ad_group_data.set("metrics", "all_conversions", 0)
            mock_ad_group_data.set("metrics", "view_through_conversions", 0)
            mock_ad_group_data.set("metrics", "video_quartile_25_rate", 0)
            mock_ad_group_data.set("metrics", "video_quartile_50_rate", 0)
            mock_ad_group_data.set("metrics", "video_quartile_75_rate", 0)
            mock_ad_group_data.set("metrics", "video_quartile_100_rate", 0)
            mock_ad_group_data.set("metrics", "engagements", engs)
            mock_ad_group_data.set("metrics", "active_view_impressions", avi)
            mock_ad_group_data.set("segments", "date", str(dt))
            mock_ad_group_data.set("segments", "device", 2, nested_key=None) # MOBILE
            mock_ad_group_data.set("segments", "ad_network_type", 6) # YOUTUBE_WATCH
            mock_ad_group_data.add_row()

        test_click_data = {}
        for dt in dates:
            resource_name = self.create_resource_name("ad_group", ad_group.id)
            key = self.create_click_report_key(resource_name, dt)
            test_click_data[key] = {
                "clicks_website": website_clicks,
                "clicks_call_to_action_overlay": call_to_action_overlay_clicks,
                "clicks_cards": cards_clicks,
                "clicks_end_cap": end_cap_clicks,
                "clicks_app_store": app_store_clicks
            }

        client = GoogleAdsClient("", "")
        updater = AdGroupUpdater(account)
        updater._get_ad_group_performance = MagicMock(return_value=mock_ad_group_data)
        updater.get_clicks_report = MagicMock(return_value=test_click_data)

        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)
        ad_group.refresh_from_db()

        self.assertEqual(ad_group.cost, sum(costs))
        self.assertEqual(ad_group.impressions, sum(impressions))
        self.assertEqual(ad_group.video_views, sum(views))
        self.assertEqual(ad_group.clicks, sum(clicks))
        self.assertEqual(ad_group.engagements, sum(engagements))
        self.assertEqual(ad_group.active_view_impressions,
                         sum(active_view_impressions))
        self.assertEqual(ad_group.clicks_website, website_clicks * dates_len)
        self.assertEqual(ad_group.clicks_call_to_action_overlay, call_to_action_overlay_clicks * dates_len)
        self.assertEqual(ad_group.clicks_app_store, app_store_clicks * dates_len)
        self.assertEqual(ad_group.clicks_cards, cards_clicks * dates_len)
        self.assertEqual(ad_group.clicks_end_cap, end_cap_clicks * dates_len)

    def test_pull_campaign_location_targeting(self):
        now = datetime(2018, 1, 15, 15, tzinfo=utc)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        geo_target = GeoTarget.objects.create(id=123, name="test name")
        geo_target.refresh_from_db()

        mock_campaign_location_data = MockGoogleAdsAPIResponse()
        mock_campaign_location_data.set("campaign_criterion", "criterion_id", geo_target.id)
        mock_campaign_location_data.set("campaign_criterion", "negative", 2)
        mock_campaign_location_data.set("campaign", "id", campaign.id)
        mock_campaign_location_data.set("metrics", "impressions", 1)
        mock_campaign_location_data.set("metrics", "video_views", 1)
        mock_campaign_location_data.set("metrics", "cost_micros", 1 * 10 ** 6)
        mock_campaign_location_data.set("metrics", "clicks", 1)
        mock_campaign_location_data.set("metrics", "conversions", 0)
        mock_campaign_location_data.set("metrics", "all_conversions", 0)
        mock_campaign_location_data.set("metrics", "view_through_conversions", 0)
        mock_campaign_location_data.set("metrics", "video_quartile_25_rate", 0)
        mock_campaign_location_data.set("metrics", "video_quartile_50_rate", 0)
        mock_campaign_location_data.set("metrics", "video_quartile_75_rate", 0)
        mock_campaign_location_data.set("metrics", "video_quartile_100_rate", 0)
        mock_campaign_location_data.add_row()

        client = GoogleAdsClient("", "")
        updater = CampaignLocationTargetUpdater(account)
        updater._get_campaign_location_target_performance = MagicMock(return_value=mock_campaign_location_data)

        updater.update(client)
        campaign.refresh_from_db()
        recalculate_de_norm_fields_for_account(account.id)

        campaign_geo_targets = campaign.geo_performance.all() \
            .values_list("geo_target_id", flat=True)
        self.assertEqual(list(campaign_geo_targets), [geo_target.id])

    def test_fulfil_placement_code_on_campaign(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1,
                                           account=account,
                                           placement_code=None,
                                           name="test",
                                           )
        test_code = "PL1234567"
        mock_hourly_performance_response = MockGoogleAdsAPIResponse()
        mock_campaign_data = MockGoogleAdsAPIResponse()
        mock_campaign_data.set("campaign", "resource_name", self.create_resource_name("campaign", campaign.id), nested_key=None)
        mock_campaign_data.set("campaign", "id", campaign.id)
        mock_campaign_data.set("campaign", "name", f"{campaign.name} {test_code}")
        mock_campaign_data.set("campaign", "status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "serving_status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
        mock_campaign_data.set("campaign", "start_date", str(today))
        mock_campaign_data.set("campaign", "end_date", str(today))
        mock_campaign_data.set("campaign_budget", "amount_micros", 0)
        mock_campaign_data.set("campaign_budget", "total_amount_micros", None)
        mock_campaign_data.set("metrics", "impressions", 0)
        mock_campaign_data.set("metrics", "video_views", 0)
        mock_campaign_data.set("metrics", "clicks", 0)
        mock_campaign_data.set("metrics", "conversions", 0)
        mock_campaign_data.set("metrics", "all_conversions", 0)
        mock_campaign_data.set("metrics", "view_through_conversions", 0)
        mock_campaign_data.set("metrics", "cost_micros", 0)
        mock_campaign_data.set("metrics", "video_quartile_25_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_50_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_75_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_100_rate", 0)
        mock_campaign_data.set("segments", "date", str(today))
        mock_campaign_data.set("segments", "device", 0, nested_key=None)
        mock_campaign_data.add_row()

        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=(mock_campaign_data, {}))
        updater._get_campaign_hourly_performance = MagicMock(return_value=mock_hourly_performance_response)

        updater.update(client)
        campaign.refresh_from_db()
        recalculate_de_norm_fields_for_account(account.id)

        campaign.refresh_from_db()
        self.assertEqual(campaign.placement_code, test_code)

    def test_pull_parent_statuses(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account,
                                           parent_parent=False,
                                           parent_not_parent=False,
                                           parent_undetermined=False,
                                           de_norm_fields_are_recalculated=True)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign,
                                          parent_parent=False,
                                          parent_not_parent=False,
                                          parent_undetermined=False,
                                          de_norm_fields_are_recalculated=True)
        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        self.assertFalse(ad_group.parent_parent)
        self.assertFalse(ad_group.parent_not_parent)
        self.assertFalse(ad_group.parent_undetermined)
        self.assertFalse(campaign.parent_parent)
        self.assertFalse(campaign.parent_not_parent)
        self.assertFalse(campaign.parent_undetermined)
        AdGroupStatistic.objects.create(
                                        id=1,
                                        date=today,
                                        ad_group=ad_group,
                                        average_position=1)

        mock_parent_data = MockGoogleAdsAPIResponse()
        for index, status in enumerate(ParentStatuses):
            mock_parent_data.set("ad_group", "id", ad_group.id)
            # Parent status ENUM 300 = PARENT, 301 = NOT_A_PARENT, 302 = UNDETERMINED
            mock_parent_data.set("ad_group_criterion", "parental_status", index + 300, nested_key="type")
            mock_parent_data.set("metrics", "impressions", 0)
            mock_parent_data.set("metrics", "video_views", 0)
            mock_parent_data.set("metrics", "clicks", 0)
            mock_parent_data.set("metrics", "conversions", 0)
            mock_parent_data.set("metrics", "all_conversions", 0)
            mock_parent_data.set("metrics", "view_through_conversions", 0)
            mock_parent_data.set("metrics", "cost_micros", 0)
            mock_parent_data.set("metrics", "video_quartile_25_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_50_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_75_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_100_rate", 0)
            mock_parent_data.set("segments", "date", str(today))
            mock_parent_data.add_row()

        client = GoogleAdsClient("", "")
        updater = ParentUpdater(account)
        updater._get_parent_performance = MagicMock(return_value=mock_parent_data)
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        self.assertTrue(ad_group.parent_parent)
        self.assertTrue(ad_group.parent_not_parent)
        self.assertTrue(ad_group.parent_undetermined)
        self.assertTrue(campaign.parent_parent)
        self.assertTrue(campaign.parent_not_parent)
        self.assertTrue(campaign.parent_undetermined)

    def test_audiences_custom_affinity(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(id=1, date=today, ad_group=ad_group,
                                        average_position=1)

        audience_id = 1234578910
        mock_audience_data = MockGoogleAdsAPIResponse()
        mock_audience_data.set("ad_group", "id", ad_group.id)
        # CriterionTypeEnum 29 = CUSTOM_AFFINITY
        mock_audience_data.set("ad_group_criterion", "type", 29, nested_key=None)
        mock_audience_data.set("ad_group_criterion", "custom_affinity.custom_affinity", f"test_custom_affinity/{audience_id}")
        mock_audience_data.set("metrics", "impressions", 0)
        mock_audience_data.set("metrics", "video_views", 0)
        mock_audience_data.set("metrics", "clicks", 0)
        mock_audience_data.set("metrics", "conversions", 0)
        mock_audience_data.set("metrics", "all_conversions", 0)
        mock_audience_data.set("metrics", "view_through_conversions", 0)
        mock_audience_data.set("metrics", "cost_micros", 0)
        mock_audience_data.set("metrics", "video_quartile_25_rate", 0)
        mock_audience_data.set("metrics", "video_quartile_50_rate", 0)
        mock_audience_data.set("metrics", "video_quartile_75_rate", 0)
        mock_audience_data.set("metrics", "video_quartile_100_rate", 0)
        mock_audience_data.set("segments", "date", str(today))
        mock_audience_data.add_row()

        client = GoogleAdsClient("", "")
        updater = InterestUpdater(account)
        updater._get_audience_performance = MagicMock(return_value=mock_audience_data)
        updater._get_audience_user_lists = MagicMock(return_value=[])
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        self.assertTrue(Audience.objects.filter(id=audience_id).exists())
        self.assertEqual(Audience.objects.get(id=audience_id).type, Audience.CUSTOM_AFFINITY_TYPE)

    def test_no_crash_on_missing_ad_group_id_in_getting_status(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(id=1, date=today, ad_group=ad_group,
                                        average_position=1)

        ad_group_ids = ["missing", ad_group.id]
        mock_parent_data = MockGoogleAdsAPIResponse()
        for _id in ad_group_ids:
            mock_parent_data.set("ad_group", "id", _id)
            mock_parent_data.set("ad_group_criterion", "parental_status", 300, nested_key="type")
            mock_parent_data.set("metrics", "cost_micros", 0)
            mock_parent_data.set("metrics", "impressions", 0)
            mock_parent_data.set("metrics", "video_views", 0)
            mock_parent_data.set("metrics", "clicks", 0)
            mock_parent_data.set("metrics", "conversions", 0)
            mock_parent_data.set("metrics", "all_conversions", 0)
            mock_parent_data.set("metrics", "view_through_conversions", 0)
            mock_parent_data.set("metrics", "video_quartile_25_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_50_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_75_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_100_rate", 0)
            mock_parent_data.set("segments", "date", str(today))
            mock_parent_data.add_row()

        self.assertEqual(ParentStatistic.objects.all().count(), 0)

        client = GoogleAdsClient("", "")
        updater = ParentUpdater(account)
        updater._get_parent_performance = MagicMock(return_value=mock_parent_data)
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        self.assertEqual(ParentStatistic.objects.all().count(), 1)

    def test_parent_enum_mapping(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account()
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(id=1, date=today, ad_group=ad_group,
                                        average_position=1)

        parent_status_response = [300, 301, 302]
        expected_parent_status_value = list(range(0, 3))
        mock_parent_data = MockGoogleAdsAPIResponse()
        for index, status in enumerate(parent_status_response):
            mock_parent_data.set("ad_group", "id", ad_group.id)
            mock_parent_data.set("ad_group_criterion", "parental_status", status, nested_key="type")
            mock_parent_data.set("metrics", "cost_micros", 0)
            mock_parent_data.set("metrics", "impressions", 0)
            mock_parent_data.set("metrics", "video_views", 0)
            mock_parent_data.set("metrics", "clicks", 0)
            mock_parent_data.set("metrics", "conversions", 0)
            mock_parent_data.set("metrics", "all_conversions", 0)
            mock_parent_data.set("metrics", "view_through_conversions", 0)
            mock_parent_data.set("metrics", "video_quartile_25_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_50_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_75_rate", 0)
            mock_parent_data.set("metrics", "video_quartile_100_rate", 0)
            mock_parent_data.set("segments", "date", str(today))
            mock_parent_data.add_row()

        self.assertEqual(ParentStatistic.objects.all().count(), 0)

        client = GoogleAdsClient("", "")
        updater = ParentUpdater(account)
        updater._get_parent_performance = MagicMock(return_value=mock_parent_data)
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        statistics = ParentStatistic.objects.all().order_by("parent_status_id")
        for index, expected in enumerate(expected_parent_status_value):
            actual = statistics[index].parent_status_id
            self.assertEqual(actual, expected)

    def test_age_range_enum_mapping(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account()
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(id=1, date=today, ad_group=ad_group,
                                        average_position=1)

        age_range_response = [503999, 503001, 503002, 503003, 503004, 503005, 503006]
        expected_age_range_values = list(range(0, 7))
        mock_age_range_data = MockGoogleAdsAPIResponse()
        for index, age_range in enumerate(age_range_response):
            mock_age_range_data.set("ad_group", "id", ad_group.id)
            mock_age_range_data.set("ad_group_criterion", "age_range", age_range, nested_key="type")
            mock_age_range_data.set("metrics", "cost_micros", 0)
            mock_age_range_data.set("metrics", "impressions", 0)
            mock_age_range_data.set("metrics", "video_views", 0)
            mock_age_range_data.set("metrics", "clicks", 0)
            mock_age_range_data.set("metrics", "conversions", 0)
            mock_age_range_data.set("metrics", "all_conversions", 0)
            mock_age_range_data.set("metrics", "view_through_conversions", 0)
            mock_age_range_data.set("metrics", "video_quartile_25_rate", 0)
            mock_age_range_data.set("metrics", "video_quartile_50_rate", 0)
            mock_age_range_data.set("metrics", "video_quartile_75_rate", 0)
            mock_age_range_data.set("metrics", "video_quartile_100_rate", 0)
            mock_age_range_data.set("segments", "date", str(today))
            mock_age_range_data.add_row()

        self.assertEqual(AgeRangeStatistic.objects.all().count(), 0)

        client = GoogleAdsClient("", "")
        updater = AgeRangeUpdater(account)
        updater._get_age_range_performance = MagicMock(return_value=mock_age_range_data)
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        statistics = AgeRangeStatistic.objects.all().order_by("age_range_id")
        for index, expected in enumerate(expected_age_range_values):
            actual = statistics[index].age_range_id
            self.assertEqual(actual, expected)

    def test_device_enum_mapping(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account()
        campaign = Campaign.objects.create(id=1, account=account)

        device_response = [1, 2, 3, 4, 5]
        expected_device_values = list(range(-1, 4))
        mock_campaign_data = MockGoogleAdsAPIResponse()
        for device_id in device_response:
            mock_campaign_data.set("campaign", "resource_name", self.create_resource_name("campaign", campaign.id),
                                   nested_key=None)
            mock_campaign_data.set("campaign", "id", campaign.id)
            mock_campaign_data.set("campaign", "name", "test")
            mock_campaign_data.set("campaign", "status", 2, nested_key=None)
            mock_campaign_data.set("campaign", "serving_status", 2, nested_key=None)
            mock_campaign_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
            mock_campaign_data.set("segments", "date", today)
            mock_campaign_data.set("campaign", "start_date", today),
            mock_campaign_data.set("campaign", "end_date", today),
            mock_campaign_data.set("campaign_budget", "amount_micros", 1 * 10 ** 6)
            mock_campaign_data.set("campaign_budget", "total_amount_micros", None)
            mock_campaign_data.set("metrics", "impressions", 1)
            mock_campaign_data.set("metrics", "video_views", 1)
            mock_campaign_data.set("metrics", "clicks", 1)
            mock_campaign_data.set("metrics", "conversions", 0)
            mock_campaign_data.set("metrics", "all_conversions", 0)
            mock_campaign_data.set("metrics", "view_through_conversions", 0)
            mock_campaign_data.set("metrics", "cost_micros", 1 * 10 ** 6)
            mock_campaign_data.set("metrics", "video_quartile_25_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_50_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_75_rate", 0)
            mock_campaign_data.set("metrics", "video_quartile_100_rate", 0)
            mock_campaign_data.set("segments", "device", device_id, nested_key=None)
            mock_campaign_data.add_row()

        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=(mock_campaign_data, {}))
        updater._get_campaign_hourly_performance = MagicMock(return_value=[])
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        statistics = CampaignStatistic.objects.all().order_by("device_id")
        for index, expected in enumerate(expected_device_values):
            actual = statistics[index].device_id
            self.assertEqual(actual, expected)

    def test_get_ad_is_disapproved(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign, name="test_ad_group")
        AdGroupStatistic.objects.create(id=1, ad_group=ad_group, date=now,
                                        average_position=1)

        # (AdGroupId, AdGroupCriterionApprovalStatusEnum)
        # AdGroupStatusEnum 0 = UNSPECIFIED, 1 = UNKNOWN, 2 = APPROVED, 3 = DISAPPROVED
        APPROVED_ID_1 = "approved_1"
        APPROVED_ID_2 = "approved_2"
        APPROVED_ID_3 = "approved_3"
        DISAPPROVED_ID_1 = "disapproved_1"

        ad_statuses = [(APPROVED_ID_1, 0), (APPROVED_ID_2, 1), (APPROVED_ID_3, 2), (DISAPPROVED_ID_1, 3)]
        mock_ad_data = MockGoogleAdsAPIResponse()
        for _id, approval_status_enum in ad_statuses:
            mock_ad_data.set("ad_group_ad", "ad.id", _id)
            mock_ad_data.set("ad_group_ad", "status", 0, nested_key=None)
            mock_ad_data.set("ad_group_ad", "ad.text_ad.headline", "test_headline")
            mock_ad_data.set("ad_group_ad", "ad.name", "test_name")
            mock_ad_data.set("ad_group_ad", "ad.display_url", "test_display_url")
            mock_ad_data.set("ad_group", "id", ad_group.id)
            mock_ad_data.set("ad_group", "name", ad_group.name)
            mock_ad_data.set("ad_group_ad", "policy_summary.approval_status", approval_status_enum, nested_key=None)
            mock_ad_data.set("metrics", "average_position", 1)
            mock_ad_data.set("metrics", "cost_micros", 0)
            mock_ad_data.set("metrics", "impressions", 0)
            mock_ad_data.set("metrics", "video_views", 0)
            mock_ad_data.set("metrics", "clicks", 0)
            mock_ad_data.set("metrics", "conversions", 0)
            mock_ad_data.set("metrics", "all_conversions", 0)
            mock_ad_data.set("metrics", "view_through_conversions", 0)
            mock_ad_data.set("metrics", "video_quartile_25_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_50_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_75_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_100_rate", 0)
            mock_ad_data.set("segments", "date", str(today))
            mock_ad_data.add_row()

        client = GoogleAdsClient("", "")
        updater = AdUpdater(account)
        updater._get_ad_performance = MagicMock(return_value=mock_ad_data)
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        def is_disapproved(ad_id):
            return Ad.objects.get(id=ad_id).is_disapproved

        self.assertFalse(is_disapproved(APPROVED_ID_1))
        self.assertFalse(is_disapproved(APPROVED_ID_2))
        self.assertFalse(is_disapproved(APPROVED_ID_3))
        self.assertTrue(is_disapproved(DISAPPROVED_ID_1))

    def test_get_ad_skip_missing_groups(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        valid_ad_group_id, invalid_ad_group_id = 1, 2
        valid_ad_id, invalid_ad_id = 3, 4
        ad_group = AdGroup.objects.create(id=valid_ad_group_id,
                                          campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=now,
                                        average_position=1)
        self.assertEqual(Ad.objects.all().count(), 0)

        mock_ad_data = MockGoogleAdsAPIResponse()
        for ad_id, ad_group_id in [(valid_ad_id, valid_ad_group_id), (invalid_ad_id, invalid_ad_group_id)]:
            mock_ad_data.set("ad_group", "id", ad_group_id)
            mock_ad_data.set("ad_group", "name", "test_name")
            mock_ad_data.set("ad_group_ad", "ad.id", ad_id)
            mock_ad_data.set("ad_group_ad", "status", 0, nested_key=None)
            mock_ad_data.set("ad_group_ad", "ad.text_ad.headline", "test_headline")
            mock_ad_data.set("ad_group_ad", "ad.name", "test_name")
            mock_ad_data.set("ad_group_ad", "ad.display_url", "test_display_url")
            mock_ad_data.set("ad_group_ad", "policy_summary.approval_status", 2, nested_key=None)
            mock_ad_data.set("metrics", "average_position", 0)
            mock_ad_data.set("metrics", "cost_micros", 0)
            mock_ad_data.set("metrics", "impressions", 0)
            mock_ad_data.set("metrics", "video_views", 0)
            mock_ad_data.set("metrics", "clicks", 0)
            mock_ad_data.set("metrics", "conversions", 0)
            mock_ad_data.set("metrics", "all_conversions", 0)
            mock_ad_data.set("metrics", "view_through_conversions", 0)
            mock_ad_data.set("metrics", "video_quartile_25_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_50_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_75_rate", 0)
            mock_ad_data.set("metrics", "video_quartile_100_rate", 0)
            mock_ad_data.set("segments", "date", str(today))
            mock_ad_data.add_row()

        client = GoogleAdsClient("", "")
        updater = AdUpdater(account)
        updater._get_ad_performance = MagicMock(return_value=mock_ad_data)
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        self.assertEqual(Ad.objects.all().count(), 1)
        self.assertIsNotNone(Ad.objects.get(id=valid_ad_id))

    def test_update_set_boolean_fields(self):
        fields = "age_18_24", "age_25_34", "age_35_44", "age_45_54", "age_55_64", "age_65", "age_undetermined", \
                 "device_computers", "device_mobile", "device_tablets", "device_other", \
                 "gender_female", "gender_male", "gender_undetermined", \
                 "has_channels", "has_interests", "has_keywords", "has_remarketing", "has_topics", "has_videos", \
                 "parent_not_parent", "parent_parent", "parent_undetermined"
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        yesterday = today - timedelta(days=1)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        for age_range_id in ALL_AGE_RANGES:
            AgeRangeStatistic.objects.create(date=yesterday, ad_group=ad_group, age_range_id=age_range_id)
            AgeRangeStatistic.objects.create(date=now, ad_group=ad_group, age_range_id=age_range_id)
        for gender_id in ALL_GENDERS:
            GenderStatistic.objects.create(date=yesterday, ad_group=ad_group, gender_id=gender_id)
            GenderStatistic.objects.create(date=now, ad_group=ad_group, gender_id=gender_id)
        for parent_id in ALL_PARENTS:
            ParentStatistic.objects.create(date=yesterday, ad_group=ad_group, parent_status_id=parent_id)
            ParentStatistic.objects.create(date=now, ad_group=ad_group, parent_status_id=parent_id)
        for device_id in ALL_DEVICES:
            CampaignStatistic.objects.create(date=yesterday, campaign=campaign, device_id=device_id)
            CampaignStatistic.objects.create(date=today, campaign=campaign, device_id=device_id)
            AdGroupStatistic.objects.create(date=yesterday, ad_group=ad_group, device_id=device_id, average_position=1)
            AdGroupStatistic.objects.create(date=today, ad_group=ad_group, device_id=device_id, average_position=1)
        audience_1 = Audience.objects.create(id=next(int_iterator))
        audience_2 = Audience.objects.create(id=next(int_iterator))
        AudienceStatistic.objects.create(date=yesterday, ad_group=ad_group, audience=audience_1)
        AudienceStatistic.objects.create(date=yesterday, ad_group=ad_group, audience=audience_2)
        AudienceStatistic.objects.create(date=today, ad_group=ad_group, audience=audience_2)
        YTChannelStatistic.objects.create(date=yesterday, ad_group=ad_group, yt_id="")
        YTChannelStatistic.objects.create(date=today, ad_group=ad_group, yt_id="")
        KeywordStatistic.objects.create(date=yesterday, ad_group=ad_group, keyword="")
        KeywordStatistic.objects.create(date=today, ad_group=ad_group, keyword="")
        remark_list_1 = RemarkList.objects.create(id=next(int_iterator))
        remark_list_2 = RemarkList.objects.create(id=next(int_iterator))
        RemarkStatistic.objects.create(date=yesterday, ad_group=ad_group, remark=remark_list_1)
        RemarkStatistic.objects.create(date=today, ad_group=ad_group, remark=remark_list_1)
        RemarkStatistic.objects.create(date=yesterday, ad_group=ad_group, remark=remark_list_2)
        topic_1 = Topic.objects.create(id=next(int_iterator))
        topic_2 = Topic.objects.create(id=next(int_iterator))
        TopicStatistic.objects.create(date=yesterday, ad_group=ad_group, topic=topic_1)
        TopicStatistic.objects.create(date=yesterday, ad_group=ad_group, topic=topic_2)
        TopicStatistic.objects.create(date=today, ad_group=ad_group, topic=topic_1)
        YTVideoStatistic.objects.create(date=yesterday, ad_group=ad_group, yt_id="")
        YTVideoStatistic.objects.create(date=today, ad_group=ad_group, yt_id="")

        recalculate_de_norm_fields_for_account(account.id)

        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        for field in fields:
            self.assertTrue(getattr(campaign, field), "Campaign.{}".format(field))
            self.assertTrue(getattr(ad_group, field), "Ad Group.{}".format(field))

    def test_update_unset_boolean_fields(self):
        fields = "age_18_24", "age_25_34", "age_35_44", "age_45_54", "age_55_64", "age_65", "age_undetermined", \
                 "device_computers", "device_mobile", "device_tablets", "device_other", \
                 "gender_female", "gender_male", "gender_undetermined", \
                 "has_channels", "has_interests", "has_keywords", "has_remarketing", "has_topics", "has_videos", \
                 "parent_not_parent", "parent_parent", "parent_undetermined"
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        account = self._create_account(now)
        common_values = {field: True for field in fields}
        campaign = Campaign.objects.create(id=1, account=account, **common_values)
        ad_group = AdGroup.objects.create(campaign=campaign, **common_values)

        recalculate_de_norm_fields_for_account(account.id)

        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        for field in fields:
            self.assertFalse(getattr(campaign, field), "Campaign. {}".format(field))
            self.assertFalse(getattr(ad_group, field), "Ad Group. {}".format(field))

    def test_first_ad_group_update_requests_report_by_yesterday(self):
        now = datetime.now(utc)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        AdGroup.objects.create(id=1,
                               campaign=campaign,
                               de_norm_fields_are_recalculated=True,
                               cost=1,
                               impressions=1,
                               video_views=1,
                               clicks=1,
                               engagements=1,
                               active_view_impressions=1)

        client = GoogleAdsClient("", "")
        updater = AdGroupUpdater(account)
        updater._get_ad_group_performance = MagicMock(return_value=[])
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        updater._get_ad_group_performance.assert_called_once()
        args = updater._get_ad_group_performance.mock_calls[0][1]
        min_date, max_date = args

        self.assertEqual(min_date, MIN_FETCH_DATE)
        self.assertEqual(max_date, now.date())

    def test_ad_group_update_requests_again_recent_statistic(self):
        now = datetime.now(utc)
        today = now.date()
        yesterday = today - timedelta(days=1)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1,
                                          campaign=campaign,
                                          de_norm_fields_are_recalculated=True,
                                          cost=1,
                                          impressions=1,
                                          video_views=1,
                                          clicks=1,
                                          engagements=1,
                                          active_view_impressions=1)
        AdGroupStatistic.objects.create(date=yesterday, ad_group=ad_group, average_position=1)

        client = GoogleAdsClient("", "")
        updater = AdGroupUpdater(account)
        updater._get_ad_group_performance = MagicMock(return_value=[])
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        updater._get_ad_group_performance.assert_called_once()
        args = updater._get_ad_group_performance.mock_calls[0][1]
        min_date, max_date = args

        self.assertEqual(min_date, MIN_FETCH_DATE)
        self.assertEqual(max_date, date.today())

    def test_ad_group_update_requests_report_by_yesterday(self):
        now = datetime.now(utc)
        today = now.date()
        last_statistic_date = today - timedelta(weeks=54)
        request_start_date = last_statistic_date + timedelta(days=1)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account, update_time=now, sync_time=now)
        ad_group = AdGroup.objects.create(id=1,
                                          campaign=campaign,
                                          de_norm_fields_are_recalculated=True,
                                          cost=1,
                                          impressions=1,
                                          video_views=1,
                                          clicks=1,
                                          engagements=1,
                                          active_view_impressions=1)
        AdGroupStatistic.objects.create(date=last_statistic_date, ad_group=ad_group, average_position=1)

        client = GoogleAdsClient("", "")
        updater = AdGroupUpdater(account)
        updater._get_ad_group_performance = MagicMock(return_value=[])
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        updater._get_ad_group_performance.assert_called_once()
        args = updater._get_ad_group_performance.mock_calls[0][1]
        min_date, max_date = args

        self.assertEqual(min_date, request_start_date)
        self.assertEqual(max_date, date.today())

    @generic_test([
        ("Updating 6am", (time(5, 59),), {}),
        ("Updating at 6am", (time(6, 0),), {}),
    ])
    def test_should_not_be_updated_until_6am(self, time_now):
        test_timezone_str = "America/Los_Angeles"
        test_timezone = timezone(test_timezone_str)
        today = date(2018, 2, 2)
        yesterday = today - timedelta(days=1)
        last_update = datetime.combine(yesterday, datetime.max.time()).replace(tzinfo=utc)
        account = self._create_account(tz=test_timezone_str, account_update_time=last_update)

        now = datetime.combine(today, time_now).replace(tzinfo=test_timezone)
        now_utc = now.astimezone(tz=utc)
        expected_update_time = now_utc

        updater = GoogleAdsUpdater()
        with patch_now(now), \
                patch("aw_reporting.google_ads.google_ads_updater.timezone.now", return_value=now_utc), \
                patch.object(updater, "main_updaters", return_value=[]):
            updater.update_all_except_campaigns(account)

        account.refresh_from_db()
        self.assertEqual(account.update_time, expected_update_time)

    def test_pre_process_chf_account_has_account_creation(self):
        chf_acc_id = "test_id"
        self.assertFalse(Account.objects.all().exists())
        self.assertFalse(AccountCreation.objects.all().exists())
        mock_accessible_accounts_data = MockGoogleAdsAPIResponse()
        mock_accessible_accounts_data.set("customer_client", "id", chf_acc_id)
        mock_accessible_accounts_data.set("customer_client", "descriptive_name", "test_customer_name")
        mock_accessible_accounts_data.set("customer_client", "manager", True)
        mock_accessible_accounts_data.set("customer_client", "test_account", False)
        mock_accessible_accounts_data.set("customer_client", "currency_code", "USD")
        mock_accessible_accounts_data.set("customer_client", "time_zone", "PST")
        mock_accessible_accounts_data.add_row()
        updater = CFAccountConnector()
        updater.get_customer_data = MagicMock(return_value=mock_accessible_accounts_data)
        updater.update()

        self.assertTrue(Account.objects.filter(id=chf_acc_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=chf_acc_id).exists())

    def test_creates_account_creation_for_customer_accounts(self):
        account = self._create_account()
        account.delete()
        test_account_id = next(int_iterator)
        self.assertFalse(Account.objects.filter(id=test_account_id).exists())

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", test_account_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "Test cid")
        mock_customer_client_data.set("customer_client", "currency_code", "USD")
        mock_customer_client_data.set("customer_client", "time_zone", "UTC")
        mock_customer_client_data.set("customer_client", "manager", False)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        mcc_accounts = Account.objects.filter(is_active=True, can_manage_clients=True)
        client = GoogleAdsClient("", "")
        for mcc in mcc_accounts:
            updater = AccountUpdater(mcc)
            updater.get_client_customer_accounts = MagicMock(return_value=mock_customer_client_data)
            updater.update(client)

        self.assertTrue(Account.objects.filter(id=test_account_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=test_account_id).exists())

    def test_get_topics_success(self):
        account = self._create_account()
        now = datetime.now(utc)
        today = now.date()
        last_update = today - timedelta(days=3)

        client = GoogleAdsClient("", "")
        updater = TopicUpdater(account)
        updater._get_topic_performance = MagicMock(return_value=[])
        updater.get_clicks_report = MagicMock(return_value={})
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=last_update, average_position=1)

        account.refresh_from_db()

    @patch("aw_reporting.google_ads.tasks.update_campaigns.cid_campaign_update")
    @patch("aw_reporting.google_ads.tasks.update_campaigns.GoogleAdsUpdater.update_accounts_for_mcc")
    def test_skip_inactive_account(self, mock_updater, mock_cid_account_update):
        self._create_account(is_active=False)
        setup_update_campaigns()
        mock_cid_account_update.assert_not_called()

    @patch.object(CampaignUpdater, "update")
    def test_mark_account_as_inactive(self, mock_update):
        account = self._create_account(is_active=True)

        exception = GoogleAdsException(None, None, MagicMock(), None)
        err = SimpleNamespace(error_code=SimpleNamespace())
        err.error_code.authorization_error = 24 # AuthorizationErrorEnum: CUSTOMER_NOT_ENABLED
        exception.failure.errors = [err]

        mock_update.side_effect = exception
        with patch("aw_reporting.google_ads.google_ads_updater.get_client", return_value=MagicMock()):
            cid_campaign_update(account.id)

        account.refresh_from_db()
        self.assertFalse(account.is_active)

    @patch.object(CampaignUpdater, "update")
    def test_success_on_account_update_error(self, mock_update):
        account = self._create_account(is_active=True)

        exception = ValueError("Test error")
        # Set attributes for google_ads_updater.execute method
        # Normal usage with updater methods will have these attributes set
        mock_update.__self__ = SimpleNamespace(__class__=SimpleNamespace())
        mock_update.side_effect = exception

        with patch("aw_reporting.google_ads.google_ads_updater.get_client", return_value=MagicMock()):
            mcc_id = Account.objects.get(can_manage_clients=True).id
            cid_campaign_update(account.id)

    @patch.object(CampaignUpdater, "update")
    def test_retry_on_error(self, mock_execute):
        mock_execute.__self__ = SimpleNamespace(__class__=SimpleNamespace())
        mock_execute.side_effect = GoogleAPIError

        test_account_id = "test_account_id"
        account = self._create_account(id=test_account_id, is_active=True)

        client = GoogleAdsClient("", "")
        updater = GoogleAdsUpdater()
        mock_updater = MagicMock()
        updater.main_updaters = (mock_updater, )
        updater.MAX_RETRIES = 2
        updater.full_update(account, client=client)
        self.assertGreater(mock_execute.call_count, 1)

    def test_budget_daily(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
            start_date=today - timedelta(days=5),
            end_date=today + timedelta(days=5),
            sync_time=datetime.now(tz=utc) + timedelta(days=5),
        )
        test_budget = 23
        statistic_date = today - timedelta(days=2)

        mock_campaign_data = MockGoogleAdsAPIResponse()
        mock_campaign_data.set("campaign", "resource_name", self.create_resource_name("campaign", campaign.id), nested_key=None)
        mock_campaign_data.set("campaign", "id", campaign.id)
        mock_campaign_data.set("campaign", "name", "test")
        mock_campaign_data.set("campaign", "status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "serving_status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
        mock_campaign_data.set("segments", "date", statistic_date)
        mock_campaign_data.set("campaign", "start_date", str(campaign.start_date))
        mock_campaign_data.set("campaign", "end_date", str(campaign.end_date))
        mock_campaign_data.set("campaign_budget", "amount_micros", test_budget * 10 ** 6)
        mock_campaign_data.set("campaign_budget", "total_amount_micros", None)
        mock_campaign_data.set("metrics", "impressions", 1)
        mock_campaign_data.set("metrics", "video_views", 1)
        mock_campaign_data.set("metrics", "clicks", 1)
        mock_campaign_data.set("metrics", "conversions", 0)
        mock_campaign_data.set("metrics", "all_conversions", 0)
        mock_campaign_data.set("metrics", "view_through_conversions", 0)
        mock_campaign_data.set("metrics", "cost_micros", 1 * 10 ** 6)
        mock_campaign_data.set("metrics", "video_quartile_25_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_50_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_75_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_100_rate", 0)
        mock_campaign_data.set("segments", "device", 0, nested_key=None)
        mock_campaign_data.add_row()

        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=(mock_campaign_data, {}))
        updater._get_campaign_hourly_performance = MagicMock(return_value=[])
        updater.update(client)

        campaign.refresh_from_db()
        self.assertAlmostEqual(campaign.budget, test_budget)
        self.assertEqual(campaign.budget_type, BudgetType.DAILY.value)

    def test_budget_total(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
            start_date=today - timedelta(days=5),
            end_date=today + timedelta(days=5),
            sync_time=datetime.now(tz=utc) + timedelta(days=5),
        )
        test_budget = 23
        statistic_date = today - timedelta(days=2)

        mock_campaign_data = MockGoogleAdsAPIResponse()
        mock_campaign_data.set("campaign", "resource_name", self.create_resource_name("campaign", campaign.id), nested_key=None)
        mock_campaign_data.set("campaign", "id", campaign.id)
        mock_campaign_data.set("campaign", "name", "test")
        mock_campaign_data.set("campaign", "status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "serving_status", 2, nested_key=None)
        mock_campaign_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
        mock_campaign_data.set("segments", "date", str(statistic_date))
        mock_campaign_data.set("campaign", "start_date", str(campaign.start_date))
        mock_campaign_data.set("campaign", "end_date", str(campaign.end_date))
        mock_campaign_data.set("campaign_budget", "amount_micros", None)
        mock_campaign_data.set("campaign_budget", "total_amount_micros", test_budget * 10 ** 6)
        mock_campaign_data.set("metrics", "impressions", 1)
        mock_campaign_data.set("metrics", "video_views", 1)
        mock_campaign_data.set("metrics", "clicks", 1)
        mock_campaign_data.set("metrics", "conversions", 0)
        mock_campaign_data.set("metrics", "all_conversions", 0)
        mock_campaign_data.set("metrics", "view_through_conversions", 0)
        mock_campaign_data.set("metrics", "cost_micros", 1 * 10 ** 6)
        mock_campaign_data.set("metrics", "video_quartile_25_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_50_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_75_rate", 0)
        mock_campaign_data.set("metrics", "video_quartile_100_rate", 0)
        mock_campaign_data.set("segments", "device", 0, nested_key=None)
        mock_campaign_data.add_row()

        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=(mock_campaign_data, {}))
        updater._get_campaign_hourly_performance = MagicMock(return_value=[])
        updater.update(client)

        campaign.refresh_from_db()
        self.assertAlmostEqual(campaign.budget, test_budget)
        self.assertEqual(campaign.budget_type, BudgetType.TOTAL.value)

    def test_account_name_limit(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/VIQ-1163
        Summary: Increase AW account name length
        """
        name_limit = 255
        mcc_account = Account.objects.create(
            id=next(int_iterator),
            timezone="UTC",
            can_manage_clients=True,
            update_time=None
        )
        AWAccountPermission.objects.create(
            account=mcc_account,
            aw_connection=AWConnection.objects.create(),
            can_read=True
        )
        test_account_id = next(int_iterator)
        self.assertFalse(Account.objects.filter(id=test_account_id).exists())

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", test_account_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "N" * name_limit)
        mock_customer_client_data.set("customer_client", "currency_code", "USD")
        mock_customer_client_data.set("customer_client", "time_zone", "UTC")
        mock_customer_client_data.set("customer_client", "manager", False)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        client = GoogleAdsClient("", "")
        updater = AccountUpdater(mcc_account)
        updater.get_client_customer_accounts = MagicMock(return_value=mock_customer_client_data)
        updater.update(client)

        self.assertTrue(Account.objects.filter(id=test_account_id).exists())
        self.assertTrue(len(Account.objects.get(id=test_account_id).name), name_limit)

    def test_db_error_retry_on_database_error(self):
        mcc_account = Account.objects.create(
            id=next(int_iterator),
            timezone="UTC",
            can_manage_clients=True,
            update_time=None
        )
        AWAccountPermission.objects.create(
            account=mcc_account,
            aw_connection=AWConnection.objects.create(),
            can_read=True
        )
        test_account_id = next(int_iterator)
        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", test_account_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "name")
        mock_customer_client_data.set("customer_client", "currency_code", "USD")
        mock_customer_client_data.set("customer_client", "time_zone", "UTC")
        mock_customer_client_data.set("customer_client", "manager", False)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        origin_method = CursorWrapper.execute

        def errors():
            yield Error("test")
            while True:
                yield None

        error_generator = errors()
        def mock_db_execute(inst, query, params=None):
            if query.startswith("INSERT INTO \"aw_reporting_account\""):
                error = next(error_generator)
                if error is not None:
                    raise error
            return origin_method(inst, query, params)

        with patch.object(CursorWrapper, "execute", autospec=True, side_effect=mock_db_execute):
            client = GoogleAdsClient("", "")
            updater = AccountUpdater(mcc_account)
            updater.get_client_customer_accounts = MagicMock(return_value=mock_customer_client_data)
            updater.update(client)

        self.assertTrue(Account.objects.filter(id=test_account_id).exists())

    def test_update_account_struck_fields(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account()
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        Ad.objects.create(id=next(int_iterator), ad_group=ad_group)
        YTChannelStatistic.objects.create(ad_group=ad_group, yt_id=str(next(int_iterator)), date=today)
        YTVideoStatistic.objects.create(ad_group=ad_group, yt_id=str(next(int_iterator)), date=today)
        AudienceStatistic.objects.create(ad_group=ad_group, date=today, audience=Audience.objects.create())
        TopicStatistic.objects.create(ad_group=ad_group, date=today, topic=Topic.objects.create())
        KeywordStatistic.objects.create(ad_group=ad_group, date=today, keyword="keyword")

        recalculate_de_norm_fields_for_account(account.id)
        account.refresh_from_db()
        self.assertGreater(account.ad_count, 0)
        self.assertGreater(account.channel_count, 0)
        self.assertGreater(account.video_count, 0)
        self.assertGreater(account.interest_count, 0)
        self.assertGreater(account.topic_count, 0)
        self.assertGreater(account.keyword_count, 0)

    def test_update_ad_group_device_tv_screen(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1,
                                          campaign=campaign,
                                          de_norm_fields_are_recalculated=True,
                                          cost=1,
                                          impressions=1,
                                          video_views=1,
                                          clicks=1,
                                          engagements=1,
                                          active_view_impressions=1,
                                          name="test"
                                          )
        dt = today - timedelta(days=2)
        resource_name = self.create_resource_name("ad_group", ad_group.id)
        mock_ad_group_data = MockGoogleAdsAPIResponse()
        mock_ad_group_data.set("campaign", "id", campaign.id)
        mock_ad_group_data.set("ad_group", "resource_name", resource_name, nested_key=None)
        mock_ad_group_data.set("ad_group", "id", ad_group.id)
        mock_ad_group_data.set("ad_group", "name", ad_group.name)
        mock_ad_group_data.set("ad_group", "status", 2, nested_key=None)
        mock_ad_group_data.set("ad_group", "type", 7, nested_key=None)
        mock_ad_group_data.set("ad_group", "cpv_bid_micros", 10000)
        mock_ad_group_data.set("ad_group", "cpm_bid_micros", None)
        mock_ad_group_data.set("ad_group", "cpc_bid_micros", None)
        mock_ad_group_data.set("metrics", "average_position", 12)
        mock_ad_group_data.set("metrics", "cost_micros", 2 * 10 ** 6)
        mock_ad_group_data.set("metrics", "impressions", 4)
        mock_ad_group_data.set("metrics", "video_views", 6)
        mock_ad_group_data.set("metrics", "clicks", 8)
        mock_ad_group_data.set("metrics", "conversions", 0)
        mock_ad_group_data.set("metrics", "all_conversions", 0)
        mock_ad_group_data.set("metrics", "view_through_conversions", 0)
        mock_ad_group_data.set("metrics", "video_quartile_25_rate", 0)
        mock_ad_group_data.set("metrics", "video_quartile_50_rate", 0)
        mock_ad_group_data.set("metrics", "video_quartile_75_rate", 0)
        mock_ad_group_data.set("metrics", "video_quartile_100_rate", 0)
        mock_ad_group_data.set("metrics", "engagements", 1)
        mock_ad_group_data.set("metrics", "active_view_impressions", 12)
        mock_ad_group_data.set("segments", "date", str(dt))
        mock_ad_group_data.set("segments", "device", DeviceEnum.Device.CONNECTED_TV, nested_key=None)
        mock_ad_group_data.set("segments", "ad_network_type", 6)
        mock_ad_group_data.add_row()

        client = GoogleAdsClient("", "")
        updater = AdGroupUpdater(account)
        updater._get_ad_group_performance = MagicMock(return_value=mock_ad_group_data)
        updater.get_clicks_report = MagicMock(return_value={})

        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)
        ad_group.refresh_from_db()

        self.assertEqual(Device.CONNECTED_TV, ad_group.statistics.first().device_id)
        self.assertTrue(ad_group.device_tv_screens)

    @patch("aw_reporting.google_ads.tasks.update_campaigns.GoogleAdsUpdater.execute")
    def test_mcc_account_access_revoked(self, mock_execute):
        account = self._create_account()
        manager = account.managers.all()[0]

        mock_execute.side_effect = RefreshError
        GoogleAdsUpdater().update_accounts_for_mcc(manager)
        manager.refresh_from_db()
        self.assertFalse(manager.is_active)

    def test_hourly_batch_process_gets_all_accounts(self):
        accounts_size = 25
        batch_size = 5
        accounts_created = set()
        accounts_seen = set()
        for i in range(accounts_size):
            cid = Account.objects.create(id=str(next(int_iterator)), is_active=True, can_manage_clients=False)
            accounts_created.add(cid.id)
        for i in range(len(accounts_created) // batch_size):
            to_update = GoogleAdsUpdater.get_accounts_to_update(hourly_update=False, size=batch_size, as_obj=True)
            for acc in to_update:
                acc.update_time = datetime.now()
                acc.save()
                accounts_seen.add(acc.id)
        self.assertEqual(accounts_created, accounts_seen)

    def test_ignore_invalid_account_ids_or_demo_accounts(self):
        accounts_size = 5
        Account.objects.create(id="demo", name="Demo", is_active=True, can_manage_clients=False)
        Account.objects.create(id="Demo", name="demo", is_active=True, can_manage_clients=False)
        Account.objects.create(id="invalid", name="Demo", is_active=True, can_manage_clients=False)
        Account.objects.create(id=str(next(int_iterator)), name="demo", is_active=True, can_manage_clients=False)
        Account.objects.create(id=str(next(int_iterator)), name="Demo", is_active=True, can_manage_clients=False)
        for i in range(accounts_size):
            Account.objects.create(id=str(next(int_iterator)), is_active=True, can_manage_clients=False)
        to_update = GoogleAdsUpdater.get_accounts_to_update(hourly_update=False, size=10, as_obj=True)
        self.assertFalse(any("demo" in acc.id.lower() or "demo" in (acc.name or "") for acc in to_update))
        errs = 0
        for acc in to_update:
            try:
                int(acc.id)
            except ValueError:
                errs += 1
        self.assertEqual(errs, 0)


class FakeExceptionWithArgs:
    def __init__(self, search_string):
        self.search_string = search_string

    def __eq__(self, other):
        return isinstance(other, ExceptionWithArgs) and self.search_string in other.args[0]
