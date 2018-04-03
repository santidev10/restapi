from datetime import datetime, date

import pytz
from django.conf import settings


def now_in_default_tz():
    return time.now()


def to_date(dt):
    """
    Cast datetime.datetime, datetime.date and string to datetime.date
    """
    if isinstance(dt, datetime):
        return dt.date()
    if isinstance(dt, date):
        return dt
    if isinstance(dt, str):
        return datetime.strptime(dt, "%Y-%m-%d").date()
    return dt


class Time:
    def now(self):
        return datetime.now(tz=pytz.timezone(settings.DEFAULT_TIMEZONE))


time = Time()
