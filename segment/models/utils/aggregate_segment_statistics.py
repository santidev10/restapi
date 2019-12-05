import logging

from django.db import connections

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS

MIN_IMPRESSIONS = 1000
MAX_STATS_TO_GET = 10000
STATISTICS_IDS_SIZE = 5000
STATS_COLUMNS = ["cost", "video_views", "clicks", "impressions"]

logger = logging.getLogger(__name__)


def aggregate_segment_statistics(related_aw_statistics_model, yt_ids):
    """
    Prepare adwords statistics for segment
    """
    table_name = str(related_aw_statistics_model._meta).replace(".", "_")
    select_fields = ", ".join(f"{table_name}.{col}" for col in STATS_COLUMNS)
    item_ids = ",".join(f"('{_id}')" for _id in yt_ids[:STATISTICS_IDS_SIZE])
    if item_ids:
        query = f"""SELECT {select_fields}, aw_reporting_adgroup.video_views AS ad_group_video_views
                FROM {table_name}
                INNER JOIN aw_reporting_adgroup
                ON ({table_name}.ad_group_id = aw_reporting_adgroup.id)
                WHERE ({table_name}.impressions >= 1000
                AND {table_name}.yt_id IN (VALUES {item_ids}))
                LIMIT {MAX_STATS_TO_GET}
                """
        try:
            with connections["default"].cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                queryset = [
                    dict(zip(columns, row))
                    for row in cursor.fetchall()
                ]
        except Exception as e:
            logger.error(e)
            queryset = []
    else:
        queryset = []

    aggregated = {
        "cost": 0,
        "video_views": 0,
        "clicks": 0,
        "impressions": 0,
        "video_clicks": 0,
        "video_impressions": 0,
    }
    for statistic in queryset:
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
