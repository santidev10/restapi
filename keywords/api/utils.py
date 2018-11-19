from collections import defaultdict

from django.db.models import Case
from django.db.models import Count
from django.db.models import ExpressionWrapper
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Value
from django.db.models import When

from aw_reporting.models import AdGroup
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import base_stats_aggregator


def get_keywords_aw_stats(accounts, keywords, fields=None):
    annotate = dict(
        campaigns_count=Count('ad_group__campaign_id', distinct=True),
        **base_stats_aggregator("ad_group__campaign__")
    )
    if fields:
        annotate = {k: v for k, v in annotate.items() if k in fields}

    account_ids = accounts.values_list("id", flat=True)
    ad_group_ids = AdGroup.objects.filter(campaign__account_id__in=account_ids)

    stats = KeywordStatistic.objects.filter(
        ad_group_id__in=ad_group_ids,
        keyword__in=keywords,
    ).values('keyword').order_by('keyword').annotate(**annotate)
    stats = {s['keyword']: s for s in stats}
    return stats


def get_keywords_aw_top_bottom_stats(accounts, keywords):
    annotate = dict(
        average_cpv=ExpressionWrapper(
            Case(
                When(
                    sum_cost__isnull=False,
                    sum_video_views__gt=0,
                    then=F("sum_cost") / F("sum_video_views"),
                ),
                output_field=FloatField()
            ),
            output_field=FloatField()
        ),
        ctr=ExpressionWrapper(
            Case(
                When(
                    sum_clicks__isnull=False,
                    sum_impressions__gt=0,
                    then=F("sum_clicks") * Value(100.0) / F("sum_impressions"),
                ),
                output_field=FloatField()
            ),
            output_field=FloatField()
        ),
        ctr_v=ExpressionWrapper(
            Case(
                When(
                    sum_clicks__isnull=False,
                    sum_video_views__gt=0,
                    then=F("sum_clicks") * Value(100.0) / F("sum_video_views"),
                ),
                output_field=FloatField()
            ),
            output_field=FloatField()
        ),
        video_view_rate=ExpressionWrapper(
            Case(
                When(
                    sum_video_views__isnull=False,
                    video_impressions__gt=0,
                    then=F("sum_video_views") * Value(100.0) / F("video_impressions"),
                ),
                output_field=FloatField()
            ),
            output_field=FloatField()
        ),
    )
    fields = list(annotate.keys())
    raw_stats = KeywordStatistic.objects.filter(
        ad_group__campaign__account__in=accounts,
        keyword__in=keywords,
    ).values("keyword", "date").order_by("keyword", "date").annotate(
        **base_stats_aggregator("ad_group__campaign__")
    ).annotate(**annotate)

    top_bottom_data = defaultdict(
        lambda: {"{}_{}".format(k, d): None
                 for k in fields for d in ("top", "bottom")}
    )
    for f in fields:
        min_field, max_field = "{}_bottom".format(f), "{}_top".format(f)
        for r in filter(lambda e: e[f] is not None, raw_stats):
            min_value, max_value = top_bottom_data[r['keyword']][min_field], top_bottom_data[r['keyword']][max_field]
            top_bottom_data[r['keyword']][min_field] = r[f] if min_value is None else min(min_value, r[f])
            top_bottom_data[r['keyword']][max_field] = r[f] \
                if max_value is None else max(top_bottom_data[r['keyword']][max_field], r[f])
    return top_bottom_data
