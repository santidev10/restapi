from datetime import timedelta

AD_WORDS_STABILITY_STATS_DAYS_COUNT = 11


def drop_latest_stats(queryset, today):
    """ Delete stats for ten days """
    date_delete = date_to_refresh_statistic(today)
    queryset.filter(date__gte=date_delete).delete()


def drop_custom_stats(queryset, min_date, max_date):
    """ Delete stats for custom range """
    queryset.filter(date__gte=min_date, date__lte=max_date).delete()


def date_to_refresh_statistic(today):
    return today - timedelta(AD_WORDS_STABILITY_STATS_DAYS_COUNT)
