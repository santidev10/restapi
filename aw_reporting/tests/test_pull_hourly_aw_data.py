from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

from django.core.management import call_command
from django.test import TransactionTestCase
from pytz import utc

from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import Devices
from utils.utils_tests import build_csv_byte_stream
from utils.utils_tests import int_iterator
from utils.utils_tests import patch_now


class PullHourlyAWDataTestCase(TransactionTestCase):
    def _call_command(self, **kwargs):
        call_command("pull_hourly_aw_data", **kwargs)

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

    def test_skip_not_existing_campaign(self):
        now = datetime(2018, 1, 1, 15, tzinfo=utc)
        today = now.date()
        account = self._create_account(now)
        campaign = Campaign.objects.create(id=1, account=account)

        common = dict(
            AveragePosition=1,
            Cost=10 ** 6,
            Date=str(today),
            Impressions=1,
            VideoViews=1,
            Clicks=1,
            Conversions=0,
            AllConversions=0,
            ViewThroughConversions=0,
            Device=Devices[0],
            VideoQuartile25Rate=0,
            VideoQuartile50Rate=0,
            VideoQuartile75Rate=0,
            VideoQuartile100Rate=0,
            Engagements=1,
            ActiveViewImpressions=1
        )

        test_report_data = [
            dict(CampaignId=campaign.id, AdGroupId=1, **common),
            dict(CampaignId="missed", AdGroupId=2, **common),
            dict(CampaignId=campaign.id, AdGroupId=3, **common),
        ]

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader()

        def mock_download(report, *_, **__):
            fields = report["selector"]["fields"]
            if report["reportType"] == "ADGROUP_PERFORMANCE_REPORT":
                return build_csv_byte_stream(fields, test_report_data)
            return build_csv_byte_stream(fields, [])

        downloader_mock.DownloadReportAsStream.side_effect = mock_download

        with patch("aw_reporting.aw_data_loader.get_web_app_client",
                   return_value=aw_client_mock):
            self._call_command()

        self.assertEqual(AdGroup.objects.all().count(), 2)
        self.assertEqual(campaign.ad_groups.count(), 2)

    def test_should_not_change_update_time(self):
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
             patch("aw_reporting.aw_data_loader.timezone.now", return_value=now), \
             patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command(empty=True)

        account.refresh_from_db()
        self.assertIsNone(account.update_time)
        self.assertEqual(account.hourly_updated_at, now)

    def test_skip_inactive_account(self):
        self._create_account(is_active=False)

        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.return_value = build_csv_byte_stream([], [])

        with patch("aw_reporting.aw_data_loader.get_web_app_client", return_value=aw_client_mock):
            self._call_command()

        downloader_mock.assert_not_called()