from unittest.mock import patch

from django.test import TestCase

from aw_reporting.models import Account
from email_reports.tasks import schedule_daily_reports


class ScheduleDailyReportsCase(TestCase):

    @patch('email_reports.tasks.send_daily_email_reports.apply_async')
    def test_send_schedule_daily_reports(self, send_daily_email_reports):
        Account.objects.create(id=1, timezone="Asia/Bangkok")
        Account.objects.create(id=2, timezone="Asia/Bangkok")
        Account.objects.create(id=3, timezone="Europe/Rome")
        Account.objects.create(id=4, timezone="Atlantic/Canary")

        reports = ["CampaignUnderMargin", "TechFeeCapExceeded", "CampaignUnderPacing", "CampaignOverPacing"]
        reports_args = "', '".join(reports)

        schedule_daily_reports(reports=reports)

        self.assertEqual(send_daily_email_reports.call_count, 3)
        call_args_list = send_daily_email_reports.call_args_list
        self.assertIn("Atlantic/Canary", str(call_args_list[0]))
        self.assertIn(reports_args, str(call_args_list[0]))

        self.assertIn("Asia/Bangkok", str(call_args_list[1]))
        self.assertIn(reports_args, str(call_args_list[1]))

        self.assertIn("Europe/Rome", str(call_args_list[2]))
        self.assertIn(reports_args, str(call_args_list[2]))
