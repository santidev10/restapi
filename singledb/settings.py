"""
SDB connector settings module
"""
DEFAULT_VIDEO_LIST_FIELDS = (
    "video_id",
    "title",
    "description",
    "views",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "likes",
    "thirty_days_likes",
    "weekly_likes",
    "daily_likes",
    "dislikes",
    "comments",
    "thirty_days_comments",
    "weekly_comments",
    "daily_comments",
    "views_history",
    "history_date",
    "thumbnail_image_url",
    "country",
    "category",
    "ptk_value",
    "language",
    "views_chart_data",
    "video_safety",
    "video_safety_status",
    "is_content_safe",
    "is_monetizable",
    "is_flagged",
    "is_brand_safety_status",
    "is_streaming",
    "flags",
    "description",
    "youtube_published_at",
    "duration",
    "has_audience",
    "verified",
    "transcript",
    "bad_words",
    "url",
    "ptk",
    "license",
    "channel__channel_id",
    "bad_statuses",
    "cms_content_owner",
    # --> disabled SAAS-1584
    # "sentiment",
    # "engage_rate",
    # <-- disabled SAAS-1584
)

DEFAULT_VIDEO_DETAILS_FIELDS = (
    "video_id",
    "title",
    "views",
    "likes",
    "dislikes",
    "comments",
    "ptk_value",
    "youtube_published_at",
    "thumbnail_image_url",
    "description",
    "country",
    "category",
    "tags",
    "chart_data",
    "thirty_days_views",
    "language",
    "is_content_safe",
    "is_monetizable",
    "is_flagged",
    "is_brand_safety_status",
    "is_streaming",
    "flags",
    "audience",
    "traffic_sources",
    "duration",
    "video_safety_status",
    "transcript",
    "bad_words",
    "ptk",
    "has_audience",
    "verified",
    "channel__category",
    "channel__channel_group",
    "channel__country",
    "channel__engage_rate",
    "channel__channel_id",
    "channel__language",
    "channel__sentiment",
    "channel__subscribers",
    "channel__thirty_days_subscribers",
    "channel__thirty_days_views",
    "channel__thumbnail_image_url",
    "channel__title",
    "channel__has_audience",
    "channel__verified",
    "channel__views_per_video",
    "aw_data",
    "license",
    "weekly_views",
    "daily_views",
    "bad_statuses",
    "cms_content_owner",
    # --> disabled SAAS-1584
    # "sentiment",
    # "engage_rate",
    # <-- disabled SAAS-1584
)

DEFAULT_CHANNEL_LIST_FIELDS = (
    "brand_safety",
    "category",
    "comments",
    "thirty_days_comments",
    "weekly_comments",
    "daily_comments",
    "ptk_value",
    "country",
    "description",
    "dislikes",
    "genre",
    "history_date",
    "channel_id",
    "url",
    "is_content_safe",
    "language",
    "likes",
    "preferred",
    "subscribers",
    "social_links",
    "thirty_days_subscribers",
    "weekly_subscribers",
    "daily_subscribers",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "thumbnail_image_url",
    "title",
    "video_views",
    "video_views_history",
    "videos",
    "views",
    "views_per_video",
    "views_per_video_history",
    "channel_group",
    "has_audience",
    "verified",
    "bad_percents",
    "bad_statuses",
    "cms_content_owner",
    # --> disabled SAAS-1584
    # "sentiment",
    # "score",
    # "score_total",
    # "engage_rate",
    # <-- disabled SAAS-1584
)

DEFAULT_CHANNEL_DETAILS_FIELDS = (
    "channel_id",
    "title",
    "ptk_value",
    "daily_views",
    "thumbnail_image_url",
    "subscribers",
    "thirty_days_subscribers",
    "thirty_days_views",
    "views_per_video",
    "views",
    "genre",
    "country",
    "category",
    "language",
    "description",
    "tags",
    "social_stats",
    "social_links",
    "chart_data",
    "safety_chart_data",
    "top_keywords",
    "emails",
    "brand_safety",
    "performance",
    "preferred",
    "is_content_safe",
    "youtube_published_at",
    "last_video_published_at",
    "updated_at",
    "channel_group",
    "audience",
    "traffic_sources",
    "has_audience",
    "verified",
    "aw_data",
    "weekly_views",
    "bad_percents",
    "bad_statuses",
    "cms_content_owner",
    # --> disabled SAAS-1584
    # "sentiment",
    # "score",
    # "score_total",
    # "engage_rate",
    # <-- disabled SAAS-1584
)

DEFAULT_KEYWORD_LIST_FIELDS = (
    "keyword",
    "updated_at",
    "daily_views",
    "weekly_views",
    "thirty_days_views",
    "views",
    "monthly_searches",
    "video_count",
    "search_volume",
    "competition",
    "average_cpc",
    "interests",
    "is_viral",
    "category",
)

DEFAULT_KEYWORD_DETAILS_FIELDS = (
    "keyword",
    "updated_at",
    "daily_views",
    "weekly_views",
    "thirty_days_views",
    "views",
    "monthly_searches",
    "video_count",
    "search_volume",
    "competition",
    "average_cpc",
    "interests",
    "is_viral",
    "category",
)

DEFAULT_VIDEO_LIST_SOURCES = (
)

DEFAULT_VIDEO_DETAILS_SOURCES = (
)

DEFAULT_CHANNEL_LIST_SOURCES = (
    "subscribers",
    "thirty_days_subscribers",
    "views_per_video",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
)

DEFAULT_CHANNEL_DETAILS_SOURCES = (
    "subscribers",
    "thirty_days_subscribers",
    "views_per_video",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "audience",
    "traffic_sources",
    "gender",
    "age_group",
    "views_history",
    "subscribers_history",
)

DEFAULT_KEYWORD_LIST_SOURCES = (
)

DEFAULT_KEYWORD_DETAILS_SOURCES = (
)

