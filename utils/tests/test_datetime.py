import pytz
from datetime import datetime
from datetime import timedelta

from django.test import TestCase

from utils.datetime import from_local_to_utc


class FromLocalToUtcCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(FromLocalToUtcCase, self).__init__(*args, **kwargs)
        self.timezone_names = (
            "Atlantic/Canary", "Asia/Bangkok", "Europe/Rome", "America/Vancouver", "America/New_York",
            "Europe/Helsinki", "Asia/Hong_Kong", "Australia/Perth", "Asia/Jakarta"
        )
        self.utc_now = datetime.now(pytz.utc)
        self.local_time = datetime(day=self.utc_now.day, month=self.utc_now.month, year=self.utc_now.year, hour=6)

    def test_not_future_time(self):
        for timezone_name in self.timezone_names:
            with self.subTest(timezone_name):

                tz = pytz.timezone(timezone_name)
                exec_time = from_local_to_utc(self.utc_now, timezone_name, self.local_time, future=False)
                self.assertEqual(exec_time.astimezone(tz).hour, 6,)
                self.assertTrue(timedelta(days=-1) <= (exec_time - self.utc_now) < timedelta())

    def test_future_time(self):
        for timezone_name in self.timezone_names:
            with self.subTest(timezone_name):

                tz = pytz.timezone(timezone_name)
                exec_time = from_local_to_utc(self.utc_now, timezone_name, self.local_time)
                self.assertEqual(exec_time.astimezone(tz).hour, 6,)
                self.assertTrue(timedelta() <= (exec_time - self.utc_now) < timedelta(days=1))
