from django.db.models import Max
from django.db.models import Min


def get_account_border_dates(account):
    from aw_reporting.models import AdGroupStatistic
    dates = AdGroupStatistic.objects.filter(
        ad_group__campaign__account=account
    ).aggregate(
        min_date=Min("date"),
        max_date=Max("date"),
    )
    return dates["min_date"], dates["max_date"]
