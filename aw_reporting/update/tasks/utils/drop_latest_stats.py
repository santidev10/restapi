from datetime import timedelta


def drop_latest_stats(queryset, today):
    # delete stats for ten days
    date_delete = date_to_refresh_statistic(today)
    queryset.filter(date__gte=date_delete).delete()


AD_WORDS_STABILITY_STATS_DAYS_COUNT = 11


def date_to_refresh_statistic(today):
    return today - timedelta(AD_WORDS_STABILITY_STATS_DAYS_COUNT)
