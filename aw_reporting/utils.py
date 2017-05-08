from datetime import timedelta


def get_dates_range(date_from, date_to):
    delta = date_to - date_from
    for i in range(delta.days + 1):
        yield date_from + timedelta(days=i)
