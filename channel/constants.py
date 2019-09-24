TERMS_FILTER = ("general_data.country", "general_data.top_language", "general_data.top_category",
                "custom_properties.preferred", "analytics.verified", "cms.cms_title",
                "stats.channel_group", "main.id")

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("social.instagram_followers", "social.twitter_followers", "social.facebook_likes",
                "stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_", "brand_safety.overall_score",
                "general_data.youtube_published_at")

EXISTS_FILTER = ("general_data.emails", "ads_stats", "analytics")

CHANNEL_CSV_HEADERS = [
    "title",
    "url",
    "country",
    "category",
    "emails",
    "subscribers",
    "thirty_days_subscribers",
    "thirty_days_views",
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