from unittest.mock import patch

from datetime import datetime
import pytz

from django.test import TestCase

from aw_reporting.models import Account
from email_reports.tasks import schedule_daily_reports
from email_reports.tasks import get_time_to_execute


class ScheduleDailyReportsCase(TestCase):

    @patch("email_reports.tasks.send_daily_email_reports.apply_async")
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


class TimeToExececuteCase(TestCase):
    def test(self):
        utc_now = datetime.now(pytz.utc)

        timezone_names = (
            "Atlantic/Canary", "Asia/Bangkok", "Europe/Rome", "America/Vancouver", "America/New_York",
            "Europe/Helsinki", "Asia/Hong_Kong", "Australia/Perth", "Asia/Jakarta"
        )

        for timezone_name in timezone_names:
            tz = pytz.timezone(timezone_name)
            exec_time = get_time_to_execute(utc_now, timezone_name)
            self.assertEqual(exec_time.astimezone(tz).hour, 6, f"{timezone_name} - {exec_time} - {exec_time.astimezone(tz)}")
            self.assertTrue(0 <= (exec_time - utc_now).total_seconds() / 3600 < 24)
