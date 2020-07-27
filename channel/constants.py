TERMS_FILTER = ("general_data.country_code", "general_data.top_lang_code", "general_data.top_category",
                "custom_properties.preferred", "analytics.verified", "main.id", "general_data.iab_categories",
                "custom_properties.is_tracked", "task_us_data.age_group", "task_us_data.content_type",
                "task_us_data.gender", "task_us_data.content_quality")

MATCH_PHRASE_FILTER = ("general_data.title", "general_data.description", "main.id")

RANGE_FILTER = ("stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers", "ads_stats.average_cpv", "ads_stats.average_cpm",
                "ads_stats.ctr", "ads_stats.ctr_v", "ads_stats.video_view_rate", "ads_stats.video_quartile_100_rate",
                "analytics.age13_17", "analytics.age18_24", "analytics.age25_34", "analytics.age35_44",
                "analytics.age45_54", "analytics.age55_64", "analytics.age65_", "brand_safety.overall_score",
                "general_data.youtube_published_at")

EXISTS_FILTER = ("monetization.is_monetizable", "task_us_data", "ads_stats")

CHANNEL_CSV_HEADERS = [
    "title",
    "url",
    "country",
    "language",
    "iab_categories",
    "subscribers",
    "thirty_days_subscribers",
    "views",
    "monthly_views",
    "weekly_views",
    "daily_views",
    "views_per_video",
    "sentiment",
    "engage_rate",
    "last_video_published_at",
    "brand_safety_score",
    "video_view_rate",
    "ctr",
    "ctr_v",
    "average_cpv",
]
