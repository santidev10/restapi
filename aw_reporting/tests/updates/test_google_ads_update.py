# pylint: disable=too-many-lines
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.db import Error
from django.db.backends.utils import CursorWrapper
from django.test import TransactionTestCase
from google.auth.exceptions import RefreshError
from googleads.errors import AdWordsReportBadRequestError
from pytz import timezone
from pytz import utc
from requests import HTTPError
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.models import AccountCreation
from aw_reporting.adwords_reports import AD_GROUP_PERFORMANCE_REPORT_FIELDS
from aw_reporting.adwords_reports import AD_PERFORMANCE_REPORT_FIELDS
from aw_reporting.adwords_reports import AWErrorType
from aw_reporting.adwords_reports import CAMPAIGN_PERFORMANCE_REPORT_FIELDS
from aw_reporting.adwords_reports import DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS
from aw_reporting.adwords_reports import DateRangeType
from aw_reporting.adwords_reports import GEO_LOCATION_REPORT_FIELDS
from aw_reporting.adwords_reports import date_formatted
from aw_reporting.google_ads.constants import MIN_FETCH_DATE
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.tasks.update_campaigns import cid_campaign_update
from aw_reporting.google_ads.tasks.update_campaigns import setup_update_campaigns
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.ads import AdUpdater
from aw_reporting.google_ads.updaters.campaign_location_target import CampaignLocationTargetUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.interests import AudienceAWType
from aw_reporting.google_ads.updaters.interests import InterestUpdater
from aw_reporting.google_ads.updaters.parents import ParentUpdater
from aw_reporting.google_ads.updaters.topics import TopicUpdater
from aw_reporting.google_ads.utils import max_ready_date
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
from aw_reporting.models import Device
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import ParentStatistic
from aw_reporting.models import ParentStatuses
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import device_str
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from utils.exception import ExceptionWithArgs
from utils.unittests.csv import build_csv_byte_stream
from utils.unittests.generic_test import generic_test
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_now import patch_now
from utils.unittests.redis_mock import MockRedis
from utils.unittests.str_iterator import str_iterator


class UpdateAwAccountsTestCase(TransactionTestCase):

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
        self.redis_mock = patch("utils.celery.tasks.REDIS_CLIENT", MockRedis())
        self.redis_mock.start()

    def tearDown(self):
        self.redis_mock.stop()

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
        costs = (2, 3)
        impressions = (4, 5)
        views = (6, 7)
        clicks = (8, 9)
        self.assertNotEqual(campaign.cost, sum(costs))
        self.assertNotEqual(campaign.impressions, sum(impressions))
        self.assertNotEqual(campaign.video_views, sum(views))
        self.assertNotEqual(campaign.clicks, sum(clicks))
        dates = (today - timedelta(days=2), today - timedelta(days=1))
        dates_len = len(dates)
        statistic = zip(dates, costs, impressions, views, clicks)
        test_statistic_data = [
            dict(
                CampaignId=str(campaign.id),
                Cost=cost * 10 ** 6,
                Date=str(dt),
                StartDate=str(campaign.start_date),
                EndDate=str(campaign.end_date),
                Amount=campaign.budget * 10 ** 6,
                TotalAmount="--",
                Impressions=impressions,
                VideoViews=views,
                Clicks=clicks,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=device_str(Device.COMPUTER),
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
            )
            for dt, cost, impressions, views, clicks in statistic
        ]
        website_clicks = 1
        call_to_action_overlay_clicks = 2
        app_store_clicks = 3
        cards_clicks = 4
        end_cap_clicks = 5
        cta_click_values = [
            {"clicks": website_clicks, "clicks_type": "Website"},
            {"clicks": call_to_action_overlay_clicks, "clicks_type": "Call-to-Action overlay"},
            {"clicks": app_store_clicks, "clicks_type": "App store"},
            {"clicks": cards_clicks, "clicks_type": "Cards"},
            {"clicks": end_cap_clicks, "clicks_type": "End cap"}
        ]
        test_cta_data = []
        for dt in dates:
            for cta_click_data in cta_click_values:
                test_cta_data.append(
                    dict(
                        CampaignId=str(campaign.id),
                        Date=str(dt),
                        Clicks=cta_click_data.get("clicks"),
                        ClickType=cta_click_data.get("clicks_type"),
                    )
                )
        statistic_fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS + ("Device", "Date")
        cta_fields = ("CampaignId", "Date", "Clicks", "ClickType")
        test_stream_statistic = build_csv_byte_stream(statistic_fields, test_statistic_data)
        test_stream_cta = build_csv_byte_stream(cta_fields, test_cta_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        def test_router(selector, *args, **kwargs):
            if "ClickType" in selector["selector"]["fields"]:
                return test_stream_cta
            return test_stream_statistic

        downloader_mock.DownloadReportAsStream = test_router
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(account).update_campaigns()

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

    # pylint: disable=too-many-locals,too-many-statements
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
                                          active_view_impressions=1)
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
        cta_click_values = [
            {"clicks": website_clicks, "clicks_type": "Website"},
            {"clicks": call_to_action_overlay_clicks, "clicks_type": "Call-to-Action overlay"},
            {"clicks": app_store_clicks, "clicks_type": "App store"},
            {"clicks": cards_clicks, "clicks_type": "Cards"},
            {"clicks": end_cap_clicks, "clicks_type": "End cap"}
        ]
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
        test_statistic_data = [
            dict(
                CampaignId=str(campaign.id),
                AdGroupId=str(ad_group.id),
                AveragePosition=1,
                Cost=cost * 10 ** 6,
                Date=str(dt),
                Impressions=impressions,
                VideoViews=views,
                Clicks=clicks,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=device_str(Device.COMPUTER),
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
                Engagements=engs,
                ActiveViewImpressions=avi,
                CpvBid="--",
                CpmBid=None,
                CpcBid=1,
            )
            for dt, cost, impressions, views, clicks, engs, avi in statistic
        ]
        test_cta_data = []
        for dt in dates:
            for cta_click_data in cta_click_values:
                test_cta_data.append(
                    dict(
                        AdGroupId=str(ad_group.id),
                        Date=str(dt),
                        Device=device_str(Device.COMPUTER),
                        Clicks=cta_click_data.get("clicks"),
                        ClickType=cta_click_data.get("clicks_type"),
                    )
                )

        statistics_fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        cta_fields = ("AdGroupId", "Date", "Device", "Clicks", "ClickType")

        test_statistic_stream = build_csv_byte_stream(statistics_fields, test_statistic_data)
        test_cta_stream = build_csv_byte_stream(cta_fields, test_cta_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        def test_router(selector, *args, **kwargs):
            if "ClickType" in selector["selector"]["fields"]:
                return test_cta_stream
            return test_statistic_stream

        downloader_mock.DownloadReportAsStream = test_router
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdGroupUpdater,)
            updater.update_all_except_campaigns()

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

    # pylint: enable=too-many-locals,too-many-statements

    def test_pull_geo_targeting(self):
        now = datetime(2018, 1, 15, 15, tzinfo=utc)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        geo_target = GeoTarget.objects.create(id=123, name="test name")
        geo_target.refresh_from_db()

        test_report_data = [
            dict(
                Id=str(geo_target.id),
                CampaignId=str(campaign.id),
                Impressions=1,
                VideoViews=1,
                Clicks=1,
                Cost=1 * 10 ** 6,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
            )
        ]
        fields = GEO_LOCATION_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (CampaignLocationTargetUpdater,)
            updater.update_all_except_campaigns()

        campaign.refresh_from_db()
        campaign_geo_targets = campaign.geo_performance.all() \
            .values_list("geo_target_id", flat=True)
        self.assertEqual(list(campaign_geo_targets), [geo_target.id])

    def test_fulfil_placement_code_on_campaign(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1,
                                           account=account,
                                           placement_code=None)

        test_code = "PL1234567"
        test_report_data = [
            dict(
                CampaignId=str(campaign.id),
                CampaignName="Campaign Name #{}, some other".format(test_code),
                Cost=0,
                Date=str(today),
                StartDate=str(today),
                EndDate=str(today),
                Amount=0,
                TotalAmount="--",
                Impressions=0,
                VideoViews=0,
                Clicks=0,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=device_str(Device.COMPUTER),
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
                BiddingStrategyType="cpv",
                ActiveViewViewability="0%",
            )
        ]

        fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS + ("Device", "Date")
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(account).update_campaigns()

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
        AdGroupStatistic.objects.create(date=today,
                                        ad_group=ad_group,
                                        average_position=1)

        test_report_data = [
            dict(
                AdGroupId=str(ad_group.id),
                Criteria=criteria,
                Date=str(today),
                Cost=0,
                Impressions=0,
                VideoViews=0,
                Clicks=0,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
            ) for criteria in ParentStatuses
        ]

        fields = DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (ParentUpdater,)
            updater.update_all_except_campaigns()

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
        AdGroupStatistic.objects.create(date=today, ad_group=ad_group,
                                        average_position=1)

        test_report_data = [
            dict(
                AdGroupId=str(ad_group.id),
                Criteria=criteria + "::123",
                Date=str(today),
                Cost=0,
                Impressions=0,
                VideoViews=0,
                Clicks=0,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
            ) for criteria in [AudienceAWType.CUSTOM_AFFINITY]
        ]

        fields = DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS + ("UserListName",)
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (InterestUpdater,)
            updater.update_all_except_campaigns()
        self.assertEqual(Audience.objects.all().count(), 1)
        self.assertEqual(Audience.objects.first().type,
                         Audience.CUSTOM_AFFINITY_TYPE)

    def test_no_crash_on_missing_ad_group_id_in_getting_status(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(date=today, ad_group=ad_group,
                                        average_position=1)

        common = dict(
            Criteria=ParentStatuses[0],
            Date=str(today),
            Cost=0,
            Impressions=0,
            VideoViews=0,
            Clicks=0,
            Conversions=0,
            AllConversions=0,
            ViewThroughConversions=0,
            VideoQuartile25Rate=0,
            VideoQuartile50Rate=0,
            VideoQuartile75Rate=0,
            VideoQuartile100Rate=0,
        )
        test_report_data = [
            dict(AdGroupId=str(99), **common),
            dict(AdGroupId=str(ad_group.id), **common)
        ]
        fields = DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        self.assertEqual(ParentStatistic.objects.all().count(), 0)

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (ParentUpdater,)
            updater.update_all_except_campaigns()

        self.assertEqual(ParentStatistic.objects.all().count(), 1)

    def test_get_ad_is_disapproved(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=now,
                                        average_position=1)
        approved_ad_1 = 1
        approved_ad_2 = 2
        approved_ad_3 = 3
        disapproved_ad_1 = 4

        common_data = dict(
            AdGroupId=str(ad_group.id),
            Date=str(today),
            AveragePosition=1,
            Cost=0,
            Impressions=0,
            VideoViews=0,
            Clicks=0,
            Conversions=0,
            AllConversions=0,
            ViewThroughConversions=0,
            VideoQuartile25Rate=0,
            VideoQuartile50Rate=0,
            VideoQuartile75Rate=0,
            VideoQuartile100Rate=0,
        )

        test_report_data = [
            dict(Id=str(approved_ad_1), **common_data),
            dict(Id=str(approved_ad_2), CombinedApprovalStatus=None, **common_data),
            dict(Id=str(approved_ad_3), CombinedApprovalStatus="any",
                 **common_data),

            dict(Id=str(disapproved_ad_1), CombinedApprovalStatus="disapproved",
                 **common_data),
        ]
        fields = AD_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdUpdater,)
            updater.update_all_except_campaigns()

        def is_disapproved(ad_id):
            return Ad.objects.get(id=ad_id).is_disapproved

        self.assertFalse(is_disapproved(approved_ad_1))
        self.assertFalse(is_disapproved(approved_ad_2))
        self.assertFalse(is_disapproved(approved_ad_3))
        self.assertTrue(is_disapproved(disapproved_ad_1))

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

        common_data = dict(
            Date=str(today),
            AveragePosition=1,
            Cost=0,
            Impressions=0,
            VideoViews=0,
            Clicks=0,
            Conversions=0,
            AllConversions=0,
            ViewThroughConversions=0,
            VideoQuartile25Rate=0,
            VideoQuartile50Rate=0,
            VideoQuartile75Rate=0,
            VideoQuartile100Rate=0,
        )

        test_report_data = [
            dict(Id=str(valid_ad_id), AdGroupId=str(valid_ad_group_id), **common_data),
            dict(Id=str(invalid_ad_id), AdGroupId=str(invalid_ad_group_id),
                 **common_data)
        ]
        fields = AD_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdUpdater,)
            updater.update_all_except_campaigns()

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

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.GoogleAdsUpdater.execute_with_any_permission"):
            updater = GoogleAdsUpdater(account)
            updater.update_all_except_campaigns()

        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        for field in fields:
            self.assertTrue(getattr(campaign, field), "Campaign. {}".format(field))
            self.assertTrue(getattr(ad_group, field), "Ad Group. {}".format(field))

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

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.GoogleAdsUpdater.execute_with_any_permission"):
            updater = GoogleAdsUpdater(account)
            updater.update_all_except_campaigns()

        campaign.refresh_from_db()
        ad_group.refresh_from_db()
        for field in fields:
            self.assertFalse(getattr(campaign, field), "Campaign. {}".format(field))
            self.assertFalse(getattr(ad_group, field), "Ad Group. {}".format(field))

    def test_first_ad_group_update_requests_report_by_yesterday(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
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

        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, [])
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdGroupUpdater,)
            updater.update_all_except_campaigns()

        downloader_mock.DownloadReportAsStream.assert_called_once_with(ANY,
                                                                       use_raw_enum_values=False,
                                                                       skip_report_header=True,
                                                                       skip_column_header=True,
                                                                       skip_report_summary=True,
                                                                       include_zero_impressions=False)
        call = downloader_mock.DownloadReportAsStream.mock_calls[0]
        payload = call[1][0]
        selector = payload["selector"]
        self.assertEqual(payload["dateRangeType"], DateRangeType.CUSTOM_DATE)
        self.assertEqual(selector["dateRange"], dict(min=date_formatted(MIN_FETCH_DATE),
                                                     max=date_formatted(today)))

    def test_ad_group_update_requests_again_recent_statistic(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
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

        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, [])
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdGroupUpdater,)
            updater.update_all_except_campaigns()

        downloader_mock.DownloadReportAsStream.assert_called_once_with(
            ANY,
            skip_report_header=True,
            skip_column_header=True,
            skip_report_summary=True,
            include_zero_impressions=False,
            use_raw_enum_values=False,
        )
        call = downloader_mock.DownloadReportAsStream.mock_calls[0]
        payload = call[1][0]
        selector = payload["selector"]
        self.assertEqual(payload["dateRangeType"], DateRangeType.CUSTOM_DATE)
        self.assertEqual(selector["dateRange"], dict(min=date_formatted(MIN_FETCH_DATE),
                                                     max=date_formatted(today)))

    def test_ad_group_update_requests_report_by_yesterday(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
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

        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, [])
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdGroupUpdater,)
            updater.update_all_except_campaigns()

        downloader_mock.DownloadReportAsStream.assert_called_once_with(ANY,
                                                                       use_raw_enum_values=False,
                                                                       skip_report_header=True,
                                                                       skip_column_header=True,
                                                                       skip_report_summary=True,
                                                                       include_zero_impressions=False)
        call = downloader_mock.DownloadReportAsStream.mock_calls[0]
        payload = call[1][0]
        selector = payload["selector"]
        self.assertEqual(payload["dateRangeType"], DateRangeType.CUSTOM_DATE)
        self.assertEqual(selector["dateRange"], dict(min=date_formatted(request_start_date),
                                                     max=date_formatted(today)))

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

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.timezone.now", return_value=now_utc), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client"), \
             patch("aw_reporting.google_ads.google_ads_updater.GoogleAdsUpdater.execute_with_any_permission"):
            updater = GoogleAdsUpdater(account)
            updater.full_update()

        account.refresh_from_db()
        self.assertEqual(account.update_time, expected_update_time)

    @generic_test([
        ("Yesterday", (3, "Etc/GMT+10", lambda utc_date, local_date: local_date < utc_date), {}),
        ("Today", (3, "UTC", lambda utc_date, local_date: local_date == utc_date), {}),
        ("Tomorrow", (22, "Etc/GMT-10", lambda utc_date, local_date: local_date > utc_date), {}),
    ])
    def test_update_always_by_yesterday(self, utc_hour, timezone_str, pre_assert_fn):
        now = datetime(2018, 2, 2, utc_hour, tzinfo=utc)
        test_timezone = timezone(timezone_str)
        local_time = now.astimezone(test_timezone)
        self.assertTrue(pre_assert_fn(now.date(), local_time.date()))

        account = self._create_account(tz=timezone_str, account_update_time=None)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = build_csv_byte_stream([], [])
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.timezone.now", return_value=now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (AdGroupUpdater, AdUpdater, ParentUpdater)
            updater.update_all_except_campaigns()

        account.refresh_from_db()
        self.assertEqual(account.update_time.astimezone(utc), now)

        expected_max_date = max_ready_date(now, tz_str=account.timezone)
        for call in downloader_mock.DownloadReportAsStream.mock_calls:
            payload = call[1][0]
            selector = payload["selector"]
            self.assertEqual(selector.get("dateRange", {}).get("max"), date_formatted(expected_max_date),
                             payload["reportName"])

    def test_creates_account_creation_for_customer_accounts(self):
        self._create_account().delete()
        mcc = Account.objects.get(can_manage_clients=True)
        test_account_id = next(int_iterator)
        self.assertFalse(Account.objects.filter(id=test_account_id).exists())

        test_customers = [
            dict(
                customerId=str(test_account_id),
                name="",
                currencyCode="",
                dateTimeZone="UTC",
                canManageClients=False,
            ),
        ]
        aw_client_mock = MagicMock()
        service_mock = aw_client_mock.GetService()
        service_mock.get.return_value = dict(entries=test_customers, totalNumEntries=len(test_customers))
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = build_csv_byte_stream((), [])

        with patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(mcc).update_accounts_as_mcc()

        self.assertTrue(Account.objects.filter(id=test_account_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=test_account_id).exists())

    def test_get_topics_success(self):
        now = datetime(2018, 2, 3, 4, 5, tzinfo=utc)
        today = now.date()
        last_update = today - timedelta(days=3)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = build_csv_byte_stream([], [])
        account = self._create_account()
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=last_update, average_position=1)

        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.timezone.now", return_value=now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            updater = GoogleAdsUpdater(account)
            updater.main_updaters = (TopicUpdater,)
            updater.update_all_except_campaigns()

        account.refresh_from_db()

    @patch("aw_reporting.google_ads.tasks.update_campaigns.cid_campaign_update")
    @patch("aw_reporting.google_ads.tasks.update_campaigns.GoogleAdsUpdater.update_accounts_as_mcc")
    def test_skip_inactive_account(self, mock_updater, mock_cid_account_update):
        self._create_account(is_active=False)
        setup_update_campaigns()
        mock_cid_account_update.assert_not_called()

    def test_mark_account_as_inactive(self):
        account = self._create_account(is_active=True)

        exception = AdWordsReportBadRequestError(AWErrorType.NOT_ACTIVE, "<null>", None, HTTP_400_BAD_REQUEST,
                                                 HTTPError(), "XML Body")

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception

        with patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock), \
             patch.object(GoogleAdsUpdater, "MAX_RETRIES", new_callable=PropertyMock(return_value=0)):
            GoogleAdsUpdater(account).update_campaigns()

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

        with patch("aw_reporting.google_ads.google_ads_updater.get_client", return_value=MagicMock()), \
             patch.object(GoogleAdsUpdater, "MAX_RETRIES", new_callable=PropertyMock(return_value=0)):
            cid_campaign_update(account.id)

    def test_budget_daily(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
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
        test_statistic_data = [
            dict(
                CampaignId=str(campaign.id),
                Cost=1 * 10 ** 6,
                Date=str(statistic_date),
                StartDate=str(campaign.start_date),
                EndDate=str(campaign.end_date),
                Amount=test_budget * 10 ** 6,
                TotalAmount="--",
                Impressions=1,
                VideoViews=1,
                Clicks=1,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=device_str(Device.COMPUTER),
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
                BiddingStrategyType="cpv",
                ActiveViewViewability="0%",
            )
        ]
        statistic_fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS + ("Device", "Date")
        test_stream_statistic = build_csv_byte_stream(statistic_fields, test_statistic_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        downloader_mock.DownloadReportAsStream.return_value = test_stream_statistic
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(account).update_campaigns()

        campaign.refresh_from_db()
        self.assertAlmostEqual(campaign.budget, test_budget)
        self.assertEqual(campaign.budget_type, BudgetType.DAILY.value)

    def test_budget_total(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
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
        test_statistic_data = [
            dict(
                CampaignId=str(campaign.id),
                Cost=1 * 10 ** 6,
                Date=str(statistic_date),
                StartDate=str(campaign.start_date),
                EndDate=str(campaign.end_date),
                TotalAmount=test_budget * 10 ** 6,
                Amount=-1,
                Impressions=1,
                VideoViews=1,
                Clicks=1,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=device_str(Device.COMPUTER),
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
                BiddingStrategyType="cpm",
                ActiveViewViewability="0%",
            )
        ]
        statistic_fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS + ("Device", "Date")
        test_stream_statistic = build_csv_byte_stream(statistic_fields, test_statistic_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        downloader_mock.DownloadReportAsStream.return_value = test_stream_statistic
        with patch_now(now), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(account).update_campaigns()

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

        test_customers = [
            dict(
                customerId=str(test_account_id),
                name="N" * name_limit,
                currencyCode="",
                dateTimeZone="UTC",
                canManageClients=False,
            ),
        ]
        aw_client_mock = MagicMock()
        service_mock = aw_client_mock.GetService()
        service_mock.get.return_value = dict(entries=test_customers, totalNumEntries=len(test_customers))

        with patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(mcc_account).update_accounts_as_mcc()

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
        test_customers = [
            dict(
                customerId=str(test_account_id),
                name="name",
                currencyCode="",
                dateTimeZone="UTC",
                canManageClients=False,
            ),
        ]

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

        aw_client_mock = MagicMock()
        service_mock = aw_client_mock.GetService()
        service_mock.get.return_value = dict(entries=test_customers, totalNumEntries=len(test_customers))
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = build_csv_byte_stream((), [])

        with patch.object(CursorWrapper, "execute", autospec=True, side_effect=mock_db_execute), \
             patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(mcc_account).update_accounts_as_mcc()

        self.assertTrue(Account.objects.filter(id=test_account_id).exists())

    def test_update_account_struck_fields(self):
        any_date = date(2019, 1, 1)
        account = self._create_account()
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        Ad.objects.create(id=next(int_iterator), ad_group=ad_group)
        YTChannelStatistic.objects.create(ad_group=ad_group, yt_id=str(next(int_iterator)), date=any_date)
        YTVideoStatistic.objects.create(ad_group=ad_group, yt_id=str(next(int_iterator)), date=any_date)
        AudienceStatistic.objects.create(ad_group=ad_group, date=any_date, audience=Audience.objects.create())
        TopicStatistic.objects.create(ad_group=ad_group, date=any_date, topic=Topic.objects.create())
        KeywordStatistic.objects.create(ad_group=ad_group, date=any_date, keyword="keyword")

        recalculate_de_norm_fields_for_account(account.id)
        account.refresh_from_db()
        self.assertGreater(account.ad_count, 0)
        self.assertGreater(account.channel_count, 0)
        self.assertGreater(account.video_count, 0)
        self.assertGreater(account.interest_count, 0)
        self.assertGreater(account.topic_count, 0)
        self.assertGreater(account.keyword_count, 0)

    def test_update_campaign_flags(self):
        any_date = date(2019, 1, 1)
        account = self._create_account()
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        Ad.objects.create(id=next(int_iterator), ad_group=ad_group)
        common = dict(ad_group=ad_group, date=any_date)
        YTChannelStatistic.objects.create(yt_id=next(str_iterator), **common)
        YTVideoStatistic.objects.create(yt_id=next(str_iterator), **common)
        AudienceStatistic.objects.create(audience=Audience.objects.create(), **common)
        TopicStatistic.objects.create(topic=Topic.objects.create(), **common)
        KeywordStatistic.objects.create(keyword="keyword", **common)
        RemarkStatistic.objects.create(remark=RemarkList.objects.create(), **common)

        recalculate_de_norm_fields_for_account(account.id)
        campaign.refresh_from_db()
        with self.subTest("has_channels"):
            self.assertTrue(campaign.has_channels)
        with self.subTest("has_videos"):
            self.assertTrue(campaign.has_videos)
        with self.subTest("has_interests"):
            self.assertTrue(campaign.has_interests)
        with self.subTest("has_topics"):
            self.assertTrue(campaign.has_topics)
        with self.subTest("has_keywords"):
            self.assertTrue(campaign.has_keywords)
        with self.subTest("has_remarketing"):
            self.assertTrue(campaign.has_remarketing)

    def test_get_accounts_to_update(self):
        now = datetime.now(utc)
        today = now.date()
        ended = today - timedelta(days=31)
        acc = Account.objects.create(id=next(int_iterator), name="account_1")

        Opportunity.objects.create(id=next(int_iterator), name="test_1", aw_cid=acc.id, end=ended)
        op_2 = Opportunity.objects.create(id=next(int_iterator), name="test_2", aw_cid=acc.id, end=today)

        pl = OpPlacement.objects.create(id=next(int_iterator), name="1", opportunity=op_2, number="test_pl", end=today)
        Campaign.objects.create(id=next(int_iterator), name="camp_1 PLtest_pl", salesforce_placement=pl,
                                account=acc)

        to_update = GoogleAdsUpdater.get_accounts_to_update()
        self.assertIn(acc.id, to_update)

    def test_revoked_oauth(self):
        account = self._create_account(is_active=True)

        exception = RefreshError("invalid_grant")
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception

        self.assertTrue(AWAccountPermission.objects.filter(
            account=account.managers.first(), aw_connection__revoked_access=False).exists())

        with patch("aw_reporting.google_ads.google_ads_updater.get_web_app_client", return_value=aw_client_mock):
            GoogleAdsUpdater(account).update_campaigns()

        account.refresh_from_db()
        self.assertFalse(AWAccountPermission.objects.filter(
            account=account.managers.first(), aw_connection__revoked_access=False).exists())

    def test_get_accounts_to_update_multiple_aw_cid(self):
        now = datetime.now(utc)
        today = now.date()
        will_end = today + timedelta(days=31)
        acc_1 = Account.objects.create(id=next(int_iterator), name="account_1", is_active=True)
        acc_2 = Account.objects.create(id=next(int_iterator), name="account_2", is_active=True)

        aw_cid = f"{acc_1.id}, {acc_2.id}"
        op_1 = Opportunity.objects.create(id=next(int_iterator), name="test_1", aw_cid=aw_cid, end=will_end)

        pl = OpPlacement.objects.create(id=next(int_iterator), name="1", opportunity=op_1, number="test_pl", end=today)
        Campaign.objects.create(id=next(int_iterator), name="camp_1 PLtest_pl", salesforce_placement=pl,
                                account=acc_1)

        to_update = GoogleAdsUpdater.get_accounts_to_update()
        self.assertIn(acc_1.id, to_update)
        self.assertIn(acc_2.id, to_update)

    def test_handle_dashes_in_cid(self):
        now = datetime.now(utc)
        today = now.date()
        will_end = today + timedelta(days=31)
        acc_id = 1234567890
        account = Account.objects.create(id=acc_id, name="account", is_active=True)

        aw_cid = "123-456-7890"
        op_1 = Opportunity.objects.create(id=next(int_iterator), name="test_1", aw_cid=aw_cid, end=will_end)

        pl = OpPlacement.objects.create(id=next(int_iterator), name="1", opportunity=op_1, number="test_pl", end=today)
        Campaign.objects.create(id=next(int_iterator), name="camp_1 PLtest_pl", salesforce_placement=pl,
                                account=account)

        to_update = GoogleAdsUpdater.get_accounts_to_update()
        self.assertIn(account.id, to_update)


class FakeExceptionWithArgs:
    def __init__(self, search_string):
        self.search_string = search_string

    def __eq__(self, other):
        return isinstance(other, ExceptionWithArgs) and self.search_string in other.args[0]
