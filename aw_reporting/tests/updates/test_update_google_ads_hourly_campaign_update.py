from datetime import date
from datetime import datetime
from datetime import timedelta
from unittest import skip
from unittest.mock import MagicMock
from unittest.mock import patch

from django.test import TransactionTestCase
from google.ads.google_ads.client import GoogleAdsClient
from pytz import utc

from aw_reporting.models import Account
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import Campaign
from aw_reporting.models import device_str
from aw_reporting.models import Device
from aw_reporting.models import Opportunity
from aw_reporting.google_ads.tasks.update_campaigns import setup_update_campaigns
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.tests.updates.test_google_ads_update import UpdateGoogleAdsTestCase
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from saas import celery_app
from utils.utittests.csv import build_csv_byte_stream
from utils.utittests.int_iterator import int_iterator
from utils.utittests.mock_google_ads_response import MockGoogleAdsAPIResponse
from utils.utittests.patch_now import patch_now


class UpdateGoogleAdsHourlyCampaignStatsTestCase(TransactionTestCase):
    def _setup(self):
        celery_app.conf.update(CELERY_ALWAYS_EAGER=True)

    def _create_account(self, manager_update_time=None, tz="UTC", account_update_time=None, **kwargs):
        mcc_account = Account.objects.create(id=next(int_iterator), timezone=tz,
                                             can_manage_clients=True,
                                             update_time=manager_update_time)
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=AWConnection.objects.create(),
                                           can_read=True)

        account = Account.objects.create(id=next(int_iterator), timezone=tz, update_time=account_update_time, **kwargs)
        account.managers.add(mcc_account)
        account.save()
        return account

    @skip
    def test_create_campaign(self):
        now = datetime.now(utc)
        today = now.date()
        account = self._create_account(now)
        campaign_id = next(int_iterator)
        self.assertFalse(Campaign.objects.filter(id=campaign_id).exists())

        mock_campaign_hourly_data = MockGoogleAdsAPIResponse()
        mock_campaign_hourly_data.set("campaign", "resource_name", UpdateGoogleAdsTestCase.create_resource_name("campaign", campaign_id), nested_key=None)
        mock_campaign_hourly_data.set("campaign", "id", campaign_id)
        mock_campaign_hourly_data.set("campaign", "name", "test")
        mock_campaign_hourly_data.set("campaign", "status", 2, nested_key=None)
        mock_campaign_hourly_data.set("campaign", "serving_status", 2, nested_key=None)
        mock_campaign_hourly_data.set("campaign", "advertising_channel_type", 0, nested_key=None)
        mock_campaign_hourly_data.set("campaign", "start_date", str(today - timedelta(days=1)))
        mock_campaign_hourly_data.set("campaign", "end_date", str(today))
        mock_campaign_hourly_data.set("campaign_budget", "amount_micros", 1 * 10 ** 6)
        mock_campaign_hourly_data.set("campaign_budget", "total_amount_micros", None)
        mock_campaign_hourly_data.set("metrics", "cost_micros", 10 ** 6)
        mock_campaign_hourly_data.set("metrics", "impressions", 1)
        mock_campaign_hourly_data.set("metrics", "video_views", 1)
        mock_campaign_hourly_data.set("metrics", "clicks", 1)
        mock_campaign_hourly_data.set("metrics", "conversions", 0)
        mock_campaign_hourly_data.set("metrics", "all_conversions", 0)
        mock_campaign_hourly_data.set("metrics", "view_through_conversions", 0)
        mock_campaign_hourly_data.set("metrics", "video_quartile_25_rate", 0)
        mock_campaign_hourly_data.set("metrics", "video_quartile_50_rate", 0)
        mock_campaign_hourly_data.set("metrics", "video_quartile_75_rate", 0)
        mock_campaign_hourly_data.set("metrics", "video_quartile_100_rate", 0)
        mock_campaign_hourly_data.set("segments", "date", str(today))
        mock_campaign_hourly_data.set("segments", "hour", 0)
        mock_campaign_hourly_data.set("segments", "device", 0, nested_key=None)
        mock_campaign_hourly_data.add_row()
        
        client = GoogleAdsClient("", "")
        updater = CampaignUpdater(account)
        updater._get_campaign_performance = MagicMock(return_value=([], {}))
        updater._get_campaign_hourly_performance = MagicMock(return_value=mock_campaign_hourly_data)
        updater.update(client)
        recalculate_de_norm_fields_for_account(account.id)

        self.assertTrue(Campaign.objects.filter(id=campaign_id).exists())

    @skip
    def test_should_not_change_update_time(self):
        test_timezone_str = "America/Los_Angeles"
        now = datetime.now(utc)
        account = self._create_account(tz=test_timezone_str)

        with patch("aw_reporting.google_ads.updaters.campaigns.timezone.now", return_value=now):
            client = GoogleAdsClient("", "")
            updater = CampaignUpdater(account)
            updater._get_campaign_performance = MagicMock(return_value=([], {}))
            updater._get_campaign_hourly_performance = MagicMock(return_value=[])
            updater.update(client)
            recalculate_de_norm_fields_for_account(account.id)

        account.refresh_from_db()
        self.assertIsNone(account.update_time)
        self.assertEqual(account.hourly_updated_at, now)

    @skip
    def test_skip_inactive_account(self):
        self._create_account(is_active=False)
        with patch("aw_reporting.google_ads.tasks.update_campaigns.GoogleAdsUpdater.update_campaigns") as mock_update_campaigns, \
                patch("aw_reporting.google_ads.tasks.update_campaigns.GoogleAdsUpdater.update_accounts_as_mcc", new=MagicMock):
            setup_update_campaigns()
            mock_update_campaigns.assert_not_called()

    def test_hourly_batch_process_gets_all_accounts(self):
        accounts_size = 25
        batch_size = 5
        accounts_created = set()
        accounts_seen = set()
        op_end = date.today() - timedelta(days=1)
        for i in range(accounts_size):
            cid = Account.objects.create(id=str(next(int_iterator)), is_active=True, can_manage_clients=False)
            Opportunity.objects.create(id=str((next(int_iterator))), name="", aw_cid=cid.id, end=op_end)
            accounts_created.add(cid.id)
        for i in range(len(accounts_created) // batch_size):
            to_update = GoogleAdsUpdater.get_accounts_to_update(hourly_update=True, size=batch_size, as_obj=True)
            for acc in to_update:
                acc.hourly_updated_at = datetime.now()
                acc.save()
                accounts_seen.add(acc.id)
        self.assertEqual(accounts_created, accounts_seen)

    def test_create_campaign_adwords(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign_id = next(int_iterator)
        self.assertFalse(Campaign.objects.filter(id=campaign_id).exists())

        test_report_data = [
            dict(
                CampaignId=campaign_id,
                AveragePosition=1,
                Cost=10 ** 6,
                Date=str(today),
                Amount=1,
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
                Engagements=1,
                ActiveViewImpressions=1,
                StartDate=str(today),
                EndDate=str(today),
                HourOfDay=0,
            ),
        ]

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        def mock_download(report, *_, **__):
            fields = report["selector"]["fields"]
            return build_csv_byte_stream(fields, test_report_data)

        downloader_mock.DownloadReportAsStream.side_effect = mock_download

        with patch("aw_reporting.google_ads.updaters.campaigns.get_web_app_client",
                   return_value=aw_client_mock):
            CampaignUpdater(account).update_hourly_campaigns()

        self.assertTrue(Campaign.objects.filter(id=campaign_id).exists())

    def test_should_not_change_update_time_adwords(self):
        test_timezone_str = "America/Los_Angeles"
        now = datetime(2018, 2, 2, 23, 55).replace(tzinfo=utc)
        account = self._create_account(tz=test_timezone_str)

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        def mock_download(report, *_, **__):
            fields = report["selector"]["fields"]
            return build_csv_byte_stream(fields, [])

        downloader_mock.DownloadReportAsStream.side_effect = mock_download

        with patch_now(now), \
             patch("aw_reporting.google_ads.updaters.campaigns.get_web_app_client", return_value=aw_client_mock):
            updater = CampaignUpdater(account)
            updater.now = now
            CampaignUpdater(account).update_hourly_campaigns()

        account.refresh_from_db()
        self.assertIsNone(account.update_time)
        self.assertEqual(account.hourly_updated_at, now)

