from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import patch

from django.core.management import call_command
from django.test import TransactionTestCase
from django.test import override_settings
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
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Device
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
from aw_reporting.models import device_str
from aw_reporting.update.tasks import AudienceAWType
from aw_reporting.update.tasks import MIN_FETCH_DATE
from aw_reporting.update.tasks import max_ready_date
from utils.exception import ExceptionWithArgs
from utils.filelock import FileLock
from utils.utils_tests import build_csv_byte_stream
from utils.utils_tests import generic_test
from utils.utils_tests import int_iterator
from utils.utils_tests import patch_now


class PullAWDataTestCase(TransactionTestCase):
    def _call_command(self, empty=False, **kwargs):
        if empty:
            kwargs["start"] = "get_ad_groups_and_stats"
            kwargs["end"] = "get_campaigns"
        call_command("pull_aw_data", **kwargs)

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
        self.acquire_mock = patch.object(FileLock, "acquire", return_value=None)
        self.release_mock = patch.object(FileLock, "release", return_value=None)
        self.acquire_mock.start()
        self.release_mock.start()

    def tearDown(self):
        self.acquire_mock.stop()
        self.release_mock.stop()

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
        cta_website = (10, 11)
        self.assertNotEqual(campaign.cost, sum(costs))
        self.assertNotEqual(campaign.impressions, sum(impressions))
        self.assertNotEqual(campaign.video_views, sum(views))
        self.assertNotEqual(campaign.clicks, sum(clicks))
        dates = (today - timedelta(days=2), today - timedelta(days=1))
        statistic = zip(dates, costs, impressions, views, clicks)
        cta = zip(dates, cta_website)
        test_statistic_data = [
            dict(
                CampaignId=campaign.id,
                Cost=cost * 10 ** 6,
                Date=str(dt),
                StartDate=str(campaign.start_date),
                EndDate=str(campaign.end_date),
                Amount=campaign.budget * 10 ** 6,
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

        test_cta_data = [
            dict(
                CampaignId=campaign.id,
                Date=str(dt),
                Clicks=clicks,
                ClickType="Website",
            )
            for dt, clicks in cta
        ]

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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(end="get_campaigns")

        campaign.refresh_from_db()
        self.assertEqual(campaign.cost, sum(costs))
        self.assertEqual(campaign.impressions, sum(impressions))
        self.assertEqual(campaign.video_views, sum(views))
        self.assertEqual(campaign.clicks, sum(clicks))
        self.assertEqual(campaign.clicks_website, sum(cta_website))

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
        cta_website = (14, 15)
        self.assertNotEqual(ad_group.cost, sum(costs))
        self.assertNotEqual(ad_group.impressions, sum(impressions))
        self.assertNotEqual(ad_group.video_views, sum(views))
        self.assertNotEqual(ad_group.clicks, sum(clicks))
        self.assertNotEqual(ad_group.engagements, sum(engagements))
        self.assertNotEqual(ad_group.active_view_impressions,
                            sum(active_view_impressions))
        dates = (today - timedelta(days=2), today - timedelta(days=1))
        statistic = zip(dates, costs, impressions, views, clicks, engagements,
                        active_view_impressions)
        cta = zip(dates, cta_website)
        test_statistic_data = [
            dict(
                CampaignId=campaign.id,
                AdGroupId=ad_group.id,
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
            )
            for dt, cost, impressions, views, clicks, engs, avi in statistic
        ]
        test_cta_data = [
            dict(
                AdGroupId=ad_group.id,
                Date=str(dt),
                Device=device_str(Device.COMPUTER),
                Clicks=clicks,
                ClickType="Website"
            )
            for dt, clicks in cta
        ]

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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(start="get_ad_groups_and_stats",
                               end="get_ad_groups_and_stats")

        ad_group.refresh_from_db()
        self.assertEqual(ad_group.cost, sum(costs))
        self.assertEqual(ad_group.impressions, sum(impressions))
        self.assertEqual(ad_group.video_views, sum(views))
        self.assertEqual(ad_group.clicks, sum(clicks))
        self.assertEqual(ad_group.engagements, sum(engagements))
        self.assertEqual(ad_group.active_view_impressions,
                         sum(active_view_impressions))
        self.assertEqual(ad_group.clicks_website, sum(cta_website))

    def test_pull_geo_targeting(self):
        now = datetime(2018, 1, 15, 15, tzinfo=utc)
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        geo_target = GeoTarget.objects.create(id=123, name="test name")
        geo_target.refresh_from_db()

        test_report_data = [
            dict(
                Id=geo_target.id,
                CampaignId=campaign.id,
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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(start="get_geo_targeting",
                               end="get_geo_targeting")

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
                CampaignId=campaign.id,
                CampaignName="Campaign Name #{}, some other".format(test_code),
                Cost=0,
                Date=str(today),
                StartDate=str(today),
                EndDate=str(today),
                Amount=0,
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
            )
        ]

        fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS + ("Device", "Date")
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(end="get_campaigns")

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
                AdGroupId=ad_group.id,
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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(start="get_parents", end="get_parents")

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
                AdGroupId=ad_group.id,
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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            call_command("pull_aw_data",
                         start="get_interests",
                         end="get_interests")

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
            dict(AdGroupId="missing", **common),
            dict(AdGroupId=ad_group.id, **common)
        ]
        fields = DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        self.assertEqual(ParentStatistic.objects.all().count(), 0)

        with patch_now(now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            call_command("pull_aw_data",
                         start="get_parents",
                         end="get_parents")

        self.assertEqual(ParentStatistic.objects.all().count(), 1)

    def test_get_ad_is_disapproved(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=now,
                                        average_position=1)
        approved_ad_1 = "approved_1"
        approved_ad_2 = "approved_2"
        approved_ad_3 = "approved_3"
        disapproved_ad_1 = "disapproved_1"

        common_data = dict(
            AdGroupId=ad_group.id,
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
            dict(Id=approved_ad_1, **common_data),
            dict(Id=approved_ad_2, CombinedApprovalStatus=None, **common_data),
            dict(Id=approved_ad_3, CombinedApprovalStatus="any",
                 **common_data),

            dict(Id=disapproved_ad_1, CombinedApprovalStatus="disapproved",
                 **common_data),
        ]
        fields = AD_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        with patch_now(now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            call_command("pull_aw_data", start="get_ads", end="get_ads")

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
            dict(Id=valid_ad_id, AdGroupId=valid_ad_group_id, **common_data),
            dict(Id=invalid_ad_id, AdGroupId=invalid_ad_group_id,
                 **common_data)
        ]
        fields = AD_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream

        with patch_now(now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            call_command("pull_aw_data", start="get_ads", end="get_ads")

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

        with patch_now(now):
            call_command("pull_aw_data", start="get_ads", end="get_videos")

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

        with patch_now(now):
            call_command("pull_aw_data", start="get_ads", end="get_videos")

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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(start="get_ad_groups_and_stats",
                               end="get_ad_groups_and_stats")

        downloader_mock.DownloadReportAsStream.assert_called_once_with(ANY,
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
             patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command(start="get_ad_groups_and_stats",
                               end="get_ad_groups_and_stats")

        downloader_mock.DownloadReportAsStream.assert_called_once_with(ANY,
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

    def test_ad_group_update_requests_report_by_yesterday(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        last_statistic_date = today - timedelta(weeks=54)
        request_start_date = last_statistic_date + timedelta(days=1)
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
        AdGroupStatistic.objects.create(date=last_statistic_date, ad_group=ad_group, average_position=1)

        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, [])
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
        with patch_now(now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command(start="get_ad_groups_and_stats",
                               end="get_ad_groups_and_stats")

        downloader_mock.DownloadReportAsStream.assert_called_once_with(ANY,
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
             patch("aw_reporting.aw_data_loader.timezone.now", return_value=now_utc), \
             patch("aw_reporting.aw_data_loader.get_web_app_client"):
            self._call_command(empty=True)

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
             patch("aw_reporting.aw_data_loader.timezone.now", return_value=now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command(end="get_cities")  # all but geo

        account.refresh_from_db()
        self.assertEqual(account.update_time.astimezone(utc), now)

        expected_max_date = max_ready_date(now, tz_str=account.timezone)
        for call in downloader_mock.DownloadReportAsStream.mock_calls:
            payload = call[1][0]
            selector = payload["selector"]
            self.assertEqual(selector.get("dateRange", {}).get("max"), date_formatted(expected_max_date),
                             payload["reportName"])

    def test_pre_process_chf_account_has_account_creation(self):
        chf_acc_id = "test_id"
        self.assertFalse(Account.objects.all().exists())
        self.assertFalse(AccountCreation.objects.all().exists())
        test_response = [
            dict(
                customerId=chf_acc_id,
                canManageClients=True,
                testAccount=False,
                descriptiveName="",
                currencyCode="",
                dateTimeZone="UTC",
            ),
        ]
        mocked_client = MagicMock()
        mocked_client.GetService().getCustomers.return_value = test_response
        with override_settings(IS_TEST=False), \
             patch("aw_reporting.adwords_api.get_client", return_value=mocked_client):
            self._call_command(start="get_ads", end="get_campaigns")

        self.assertTrue(Account.objects.filter(id=chf_acc_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=chf_acc_id).exists())

    def test_creates_account_creation_for_customer_accounts(self):
        self._create_account().delete()
        test_account_id = next(int_iterator)
        self.assertFalse(Account.objects.filter(id=test_account_id).exists())

        test_customers = [
            dict(
                customerId=test_account_id,
                name="",
                currencyCode="",
                dateTimeZone="UTC",
            ),
        ]
        aw_client_mock = MagicMock()
        service_mock = aw_client_mock.GetService()
        service_mock.get.return_value = dict(entries=test_customers, totalNumEntries=len(test_customers))
        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command(start="get_ads", end="get_campaigns")

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
             patch("aw_reporting.aw_data_loader.timezone.now", return_value=now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command(start="get_topics", end="get_topics")

        account.refresh_from_db()

    def test_skip_inactive_account(self):
        self._create_account(is_active=False)

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.return_value = build_csv_byte_stream([], [])

        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command()

        downloader_mock.assert_not_called()

    def test_mark_account_as_inactive(self):
        account = self._create_account(is_active=True)

        exception = AdWordsReportBadRequestError(AWErrorType.NOT_ACTIVE, "<null>", None, HTTP_400_BAD_REQUEST,
                                                 HTTPError(), 'XML Body')

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception

        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command()

        account.refresh_from_db()
        self.assertFalse(account.is_active)

    def test_success_on_account_update_error(self):
        self._create_account(is_active=True)

        exception = ValueError("Test error")

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception

        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock), \
             patch.object(FileLock, "release") as release_mock, \
                patch("aw_reporting.adwords_reports.MAX_ACCESS_AD_WORDS_TRIES", 0):
            self._call_command()

        release_mock.assert_called_with()

    def test_emails_error(self):
        test_account_id = "test_account_id"
        self._create_account(id=test_account_id, is_active=True)

        exception = ValueError("Test error")

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception

        from aw_reporting.update.update_aw_account import logger

        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock), \
             patch("aw_reporting.adwords_reports.MAX_ACCESS_AD_WORDS_TRIES", 0), \
             patch.object(logger, "exception") as exception_mock:
            self._call_command(account_ids=test_account_id)

        exception_mock.assert_called_with(FakeExceptionWithArgs(test_account_id))


class FakeExceptionWithArgs:
    def __init__(self, search_string):
        self.search_string = search_string

    def __eq__(self, other):
        return isinstance(other, ExceptionWithArgs) and self.search_string in other.args[0]
