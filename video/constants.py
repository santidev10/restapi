TERMS_FILTER = ("general_data.country", "general_data.country_code", "general_data.language",
                "general_data.lang_code", "general_data.category",
                "analytics.verified", "channel.id", "channel.title",
                "monetization.is_monetizable", "monetization.channel_preferred",
                "channel.id", "general_data.tags", "main.id", "general_data.iab_categories",
                "task_us_data.age_group", "task_us_data.content_type", "task_us_data.gender")

MATCH_PHRASE_FILTER = ("general_data.title", "general_data.description")

RANGE_FILTER = ("stats.views", "stats.engage_rate", "stats.sentiment", "stats.views_per_day",
                "stats.channel_subscribers", "ads_stats.average_cpv", "ads_stats.average_cpm", "ads_stats.ctr_v",
                "ads_stats.ctr", "ads_stats.video_view_rate", "ads_stats.video_quartile_100_rate", "analytics.age13_17",
                "analytics.age18_24", "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_", "general_data.youtube_published_at",
                "stats.last_day_views", "brand_safety.overall_score")

EXISTS_FILTER = ("ads_stats", "stats.flags", "custom_captions.items", "transcripts", "task_us_data")

HISTORY_FIELDS = ("stats.views_history", "stats.likes_history", "stats.dislikes_history",
                  "stats.comments_history", "stats.historydate",)

VIDEO_CSV_HEADERS = [
    "title",
    "url",
    "iab_categories",
    "views",
    "monthly_views",
    "weekly_views",
    "daily_views",
    "likes",
    "dislikes",
    "comments",
    "youtube_published_at",
    "brand_safety_score",
    "video_view_rate",
    "ctr",
    "ctr_v",
    "average_cpv",
]
