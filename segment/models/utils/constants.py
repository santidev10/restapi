shared_avg_aggs = (
    "stats.last_30day_views",
    "brand_safety.overall_score",
    "stats.views",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.video_view_rate",
    "ads_stats.average_cpm",
    "ads_stats.average_cpv",
)
shared_sum_aggs = (
    "stats.views",
    "stats.observed_videos_likes",
)
video_sum_aggs = (
    "stats.likes",
    "stats.dislikes",
    *shared_sum_aggs,
)
channel_sum_aggs = (
    "stats.observed_videos_likes",
    "stats.observed_videos_dislikes",
    "stats.last_30day_subscribers",
    "stats.subscribers",
    "brand_safety.videos_scored",
    *shared_sum_aggs,
)
