from datetime import datetime

import pytz


def max_ready_date(dt: datetime, tz=None, tz_str="UTC"):
    tz = tz or pytz.timezone(tz_str)
    return dt.astimezone(tz).date()
