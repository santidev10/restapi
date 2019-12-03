from django.db import connection

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS

MIN_IMPRESSIONS = 1000
COLUMNS = ["cost", "video_views", "clicks", "impressions", "video_views"]


def aggregate_segment_statistics(related_aw_statistics_model, yt_ids):
    """
    Prepare adwords statistics for segment
    """
    table_name = str(related_aw_statistics_model._meta).replace(".", "_")
    select_fields = ", ".join(f"{table_name}.{col}" for col in COLUMNS)
    yt_ids = ",".join("'{}'".format(_id) for _id in yt_ids)
    query = f"""SELECT {select_fields} 
            AS ad_group_video_views
            FROM {table_name} 
            INNER JOIN aw_reporting_adgroup
            ON ({table_name}.ad_group_id = aw_reporting_adgroup.id) 
            WHERE ({table_name}.impressions >= {MIN_IMPRESSIONS} AND {table_name}.yt_id IN ({yt_ids}))"""
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        results = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]
        aggregated = {
            "cost": 0,
            "video_views": 0,
            "clicks": 0,
            "impressions": 0,
            "video_clicks": 0,
            "video_impressions": 0,
        }
        for statistic in results:
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
