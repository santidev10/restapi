"""
Segment models utils module
"""
from django.db.models import F
from utils.datetime import now_in_default_tz
from segment.models.utils.count_segment_adwords_statistics import get_mcc_to_update

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS


def aggregate_segment_statistics(segment):
    """
    Prepare adwords statistics for segment
    """
    user = segment.owner
    mcc_acc, is_chf = get_mcc_to_update(user)
    filters = {
        "ad_group__campaign__account__managers": mcc_acc,
        "yt_id__in": segment.related_ids,
    }
    aggregated = {
        "cost": 0,
        "video_views": 0,
        "clicks": 0,
        "impressions": 0,
        "video_clicks": 0,
        "video_impressions": 0,
    }
    queryset = segment.related_aw_statistics_model.objects.filter(**filters).annotate(ad_group_video_views=F("ad_group__video_views"), video_clicks=F("clicks"), video_impressions=F("impressions")).values("cost", "video_views", "clicks", "impressions", "ad_group_video_views", "video_clicks", "video_impressions")
    queryset.query.clear_ordering(force_empty=True)
    for statistic in queryset:
        for field, value in statistic.items():
            if field == "ad_group_video_views":
                continue
            elif field == "video_clicks" or field == "video_impressions" and statistic["ad_group_video_views"] > 0:
                aggregated[field] += value
            else:
                aggregated[field] += value
    stats = {}
    for key, opts in CALCULATED_STATS.items():
        kwargs = {
            key: aggregated[key] for key in opts["args"]
        }
        func = opts["receipt"]
        result = func(**kwargs)
        stats[key] = result
    result = {
        "stats": stats,
        "meta": {
            "account_id": mcc_acc.id,
            "account_name": mcc_acc.name,
            "updated_at": str(now_in_default_tz()),
            "is_chf": is_chf,
        }
    }
    return result
