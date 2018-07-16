from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from django.core.management import call_command
from django.test import TransactionTestCase
from pytz import utc

from aw_reporting.adwords_reports import CAMPAIGN_PERFORMANCE_REPORT_FIELDS, \
    AD_GROUP_PERFORMANCE_REPORT_FIELDS, GEO_LOCATION_REPORT_FIELDS, \
    DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS, AD_PERFORMANCE_REPORT_FIELDS
from aw_reporting.models import Campaign, Account, AWConnection, \
    AWAccountPermission, Devices, AdGroup, GeoTarget, ParentStatuses, \
    AdGroupStatistic, Audience, Ad, ParentStatistic, AgeRangeStatistic, GenderStatistic, ALL_AGE_RANGES, ALL_GENDERS, \
    ALL_PARENTS, ALL_DEVICES, CampaignStatistic, AudienceStatistic, YTChannelStatistic, KeywordStatistic, \
    YTVideoStatistic, RemarkStatistic, RemarkList, Topic, TopicStatistic
from aw_reporting.tasks import AudienceAWType
from utils.utils_tests import patch_now, build_csv_byte_stream, int_iterator


class PullAWDataTestCase(TransactionTestCase):
    def _call_command(self, **kwargs):
        call_command("pull_aw_data", **kwargs)

    def _create_account(self, update_time):
        connection = AWConnection.objects.create()
        mcc_account = Account.objects.create(id=1, timezone="UTC",
                                             can_manage_clients=True,
                                             update_time=update_time)
        permission = AWAccountPermission.objects.create(account=mcc_account,
                                                        aw_connection=connection,
                                                        can_read=True)
        mcc_account.mcc_permissions.add(permission)
        mcc_account.save()

        account = Account.objects.create(id=2, timezone="UTC")
        account.managers.add(mcc_account)
        account.save()
        return account

    def test_update_campaign_aggregated_stats(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1,
                                           account=account,
                                           de_norm_fields_are_recalculated=True,
                                           start_date=today - timedelta(
                                               days=5),
                                           end_date=today + timedelta(days=5),
                                           cost=1,
                                           budget=1,
                                           impressions=1,
                                           video_views=1,
                                           clicks=1)
        costs = (2, 3)
        impressions = (4, 5)
        views = (6, 7)
        clicks = (8, 9)
        self.assertNotEqual(campaign.cost, sum(costs))
        self.assertNotEqual(campaign.impressions, sum(impressions))
        self.assertNotEqual(campaign.video_views, sum(views))
        self.assertNotEqual(campaign.clicks, sum(clicks))
        dates = (today - timedelta(days=2), today - timedelta(days=1))
        statistic = zip(dates, costs, impressions, views, clicks)
        test_report_data = [
            dict(
                CampaignId=campaign.id,
                Cost=cost * 10 ** 6,
                Date=str(date),
                StartDate=str(campaign.start_date),
                EndDate=str(campaign.end_date),
                Amount=campaign.budget * 10 ** 6,
                Impressions=impressions,
                VideoViews=views,
                Clicks=clicks,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=Devices[0],
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
            )
            for date, cost, impressions, views, clicks in statistic
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
        self.assertEqual(campaign.cost, sum(costs))
        self.assertEqual(campaign.impressions, sum(impressions))
        self.assertEqual(campaign.video_views, sum(views))
        self.assertEqual(campaign.clicks, sum(clicks))

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
        test_report_data = [
            dict(
                CampaignId=campaign.id,
                AdGroupId=ad_group.id,
                AveragePosition=1,
                Cost=cost * 10 ** 6,
                Date=str(date),
                Impressions=impressions,
                VideoViews=views,
                Clicks=clicks,
                Conversions=0,
                AllConversions=0,
                ViewThroughConversions=0,
                Device=Devices[0],
                VideoQuartile25Rate=0,
                VideoQuartile50Rate=0,
                VideoQuartile75Rate=0,
                VideoQuartile100Rate=0,
                Engagements=engs,
                ActiveViewImpressions=avi,
            )
            for date, cost, impressions, views, clicks, engs, avi in statistic
        ]

        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS
        test_stream = build_csv_byte_stream(fields, test_report_data)
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()
        downloader_mock.DownloadReportAsStream.return_value = test_stream
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
                Device=Devices[0],
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
