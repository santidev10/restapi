import logging

from django.db.models import F

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS

MIN_IMPRESSIONS = 1000
MAX_STATS_TO_GET = 5000
STATISTICS_IDS_SIZE = 2500

logger = logging.getLogger(__name__)


def aggregate_segment_statistics(related_aw_statistics_model, yt_ids):
    """
    Prepare adwords statistics for segment
    """
    filters = {
        "yt_id__in": yt_ids[:STATISTICS_IDS_SIZE],
        "impressions__gte": MIN_IMPRESSIONS,
    }
    aggregated = {
        "cost": 0,
        "video_views": 0,
        "clicks": 0,
        "impressions": 0,
        "video_clicks": 0,
        "video_impressions": 0,
    }
    queryset = related_aw_statistics_model.objects\
        .select_related("ad_group")\
        .only("ad_group__video_views").filter(**filters)\
        .annotate(ad_group_video_views=F("ad_group__video_views"), video_clicks=F("clicks"), video_impressions=F("impressions"))\
        .values("cost", "video_views", "clicks", "impressions", "ad_group_video_views", "video_clicks", "video_impressions")
    queryset.query.clear_ordering(force_empty=True)
    for statistic in queryset[:MAX_STATS_TO_GET]:
        if statistic["ad_group_video_views"] > 0:
            aggregated["video_clicks"] += statistic["clicks"]
            aggregated["video_impressions"] += statistic["impressions"]
        aggregated["cost"] += statistic["cost"]
        aggregated["video_views"] += statistic["video_views"]
        aggregated["clicks"] += statistic["clicks"]
        aggregated["impressions"] += statistic["impressions"]
    stats = {}
    for key, opts in CALCULATED_STATS.items():
        kwargs = {
            key: aggregated[key] for key in opts["args"]
        }
        func = opts["receipt"]
        result = func(**kwargs)
        stats[key] = result
    return stats
