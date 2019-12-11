from django.db.models import Sum

from aw_reporting.models import base_stats_aggregator


def stats_aggregator(ad_group_stats_prefix=None, prefix=None):
    _base_stats_aggregator = base_stats_aggregator(prefix)
    _base_stats_aggregator.update(
        sum_all_conversions=Sum("{}all_conversions".format(ad_group_stats_prefix if ad_group_stats_prefix else ""))
    )
    return _base_stats_aggregator
