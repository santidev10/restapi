"""
Segment models utils module
"""
from django.db.models import F

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS


def aggregate_segment_statistics(related_aw_statistics_model, yt_ids):
    """
    Prepare adwords statistics for segment
    """
    filters = {
        "yt_id__in": yt_ids,
    }
    aggregated = {
        "cost": 0,
        "video_views": 0,
        "clicks": 0,
        "impressions": 0,
        "video_clicks": 0,
        "video_impressions": 0,
    }
    queryset = related_aw_statistics_model.objects.filter(**filters).annotate(ad_group_video_views=F("ad_group__video_views"), video_clicks=F("clicks"), video_impressions=F("impressions")).values("cost", "video_views", "clicks", "impressions", "ad_group_video_views", "video_clicks", "video_impressions")
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
    return stats
