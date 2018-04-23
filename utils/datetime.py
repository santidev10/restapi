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


def build_periods(quarters=None, start=None, end=None, compare_yoy=False,
                  today=None):
    """
    Get datetime periods by combination of query params
    :param quarters: list of literals {Q1, Q2, Q3, Q4}
    :param start: datetime of start
    :param end: datetime of end
    :param compare_yoy: flag "Compare Year to Year"
    :param today:
    :return: list of periods
    """
    today = today or now_in_default_tz().date()
    if isinstance(quarters, list):
        return _build_periods_by_quarters(quarters, today, compare_yoy)
    elif compare_yoy:
        year = today.year
        return [(date(year - 1, 1, 1), date(year, 12, 31))]
    elif start is not None and end is not None:
        return [(start, end)]
    return []


def _build_periods_by_quarters(quarters, today, compare_yoy=False):
    start_of_period = None
    periods = []
    month = today.month
    year = today.year
    for i in range(1, 5):
        quarter = "Q%d" % i
        if quarter in quarters:
            current_quarter_days = quarter_days[quarter]
            quarter_year = year - 1 \
                if month < current_quarter_days[0][0] else year
            next_quarter = "Q%d" % (i + 1)
            if next_quarter in quarters:
                next_quarter_days = quarter_days[next_quarter]
                next_quarter_year = year - 1 \
                    if month < next_quarter_days[0][0] else year
            else:
                next_quarter_year = None

            if next_quarter_year is None or next_quarter_year != quarter_year:
                start = start_of_period or datetime(quarter_year,
                                                    *current_quarter_days[
                                                        0]).date()
                end = datetime(quarter_year, *current_quarter_days[1]).date()

                periods.append((start, end))
                start_of_period = None

            elif start_of_period is None:
                start_of_period = datetime(quarter_year,
                                           *current_quarter_days[0]).date()
    if compare_yoy:
        max_year = max(e for _, e in periods).year
        min_year = max_year - 1

        pairs = [(_period_to_year(p, min_year), _period_to_year(p, max_year))
                 for p in periods]
        periods = [item for sublist in pairs for item in sublist]
    return sorted(periods)


quarter_days = dict(
    Q1=((1, 1), (3, 31)),
    Q2=((4, 1), (6, 30)),
    Q3=((7, 1), (9, 30)),
    Q4=((10, 1), (12, 31)),
)


def _period_to_year(period, year):
    start, end = period
    return _date_to_year(start, year), _date_to_year(end, year)


def _date_to_year(d, year):
    return date(year, d.month, d.day)
