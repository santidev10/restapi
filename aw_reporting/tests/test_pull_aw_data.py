import csv
import io
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from django.core.management import call_command
from pytz import utc
from rest_framework.test import APITestCase

from aw_reporting.adwords_reports import CAMPAIGN_PERFORMANCE_REPORT_FIELDS, \
    AD_GROUP_PERFORMANCE_REPORT_FIELDS
from aw_reporting.models import Campaign, Account, AWConnection, \
    AWAccountPermission, Devices, AdGroup
from utils.utils_tests import patch_now


class PullAWDataTestCase(APITestCase):
    def test_update_campaign_aggregated_stats(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        connection = AWConnection.objects.create()
        mcc_account = Account.objects.create(id=1, timezone="UTC",
                                             can_manage_clients=True,
                                             update_time=now)
        permission = AWAccountPermission.objects.create(account=mcc_account,
                                                        aw_connection=connection,
                                                        can_read=True)
        mcc_account.mcc_permissions.add(permission)
        mcc_account.save()

        account = Account.objects.create(id=2, timezone="UTC")
        account.managers.add(mcc_account)
        campaign = Campaign.objects.create(id=1,
                                           account=account,
                                           de_norm_fields_are_recalculated=True,
                                           start_date=today - timedelta(days=5),
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
            call_command("pull_aw_data", end="get_campaigns")

        campaign.refresh_from_db()
        self.assertEqual(campaign.cost, sum(costs))
        self.assertEqual(campaign.impressions, sum(impressions))
        self.assertEqual(campaign.video_views, sum(views))
        self.assertEqual(campaign.clicks, sum(clicks))

    def test_update_ad_group_aggregated_stats(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        connection = AWConnection.objects.create()
        mcc_account = Account.objects.create(id=1, timezone="UTC",
                                             can_manage_clients=True,
                                             update_time=now)
        permission = AWAccountPermission.objects.create(account=mcc_account,
                                                        aw_connection=connection,
                                                        can_read=True)
        mcc_account.mcc_permissions.add(permission)
        mcc_account.save()

        account = Account.objects.create(id=2, timezone="UTC")
        account.managers.add(mcc_account)
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
            call_command("pull_aw_data", start="get_ad_groups_and_stats",
                         end="get_ad_groups_and_stats")

        ad_group.refresh_from_db()
        self.assertEqual(ad_group.cost, sum(costs))
        self.assertEqual(ad_group.impressions, sum(impressions))
        self.assertEqual(ad_group.video_views, sum(views))
        self.assertEqual(ad_group.clicks, sum(clicks))
        self.assertEqual(ad_group.engagements, sum(engagements))
        self.assertEqual(ad_group.active_view_impressions,
                         sum(active_view_impressions))


def build_csv_byte_stream(headers, rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    for row in rows:
        clean_row = dict([(k, v) for k, v in row.items() if k in headers])
        writer.writerow(clean_row)
    output.seek(0)
    text_csv = output.getvalue()
    stream = io.BytesIO(text_csv.encode())
    return stream
