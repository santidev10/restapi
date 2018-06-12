from datetime import datetime
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from pytz import utc
from rest_framework.test import APITransactionTestCase

from aw_reporting.models import AWConnection, Account, AWAccountPermission, \
    Campaign, Devices, AdGroup
from utils.utils_tests import build_csv_byte_stream


class PullHourlyAWDataTestCase(APITransactionTestCase):
    def _call_command(self, **kwargs):
        call_command("pull_hourly_aw_data", **kwargs)

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
