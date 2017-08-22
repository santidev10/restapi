from aw_reporting.models import KeywordStatistic, base_stats_aggregate
from django.db.models import Count


def get_keywords_aw_stats(accounts, keywords, fields=None):
    annotate = dict(
        campaigns_count=Count('ad_group__campaign_id', distinct=True),
        **base_stats_aggregate
    )
    if fields:
        annotate = {k: v for k, v in annotate.items() if k in fields}

    stats = KeywordStatistic.objects.filter(
        ad_group__campaign__account__in=accounts, keyword__in=keywords,
    ).values('keyword').order_by('keyword').annotate(**annotate)
    stats = {s['keyword']: s for s in stats}
    return stats
