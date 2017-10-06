"""
SDB connector settings module
"""
DEFAULT_VIDEO_LIST_FIELDS = (
    "video_id",
    "title",
    "description",
    "views",
    "likes",
    "dislikes",
    "comments",
    "views_history",
    "history_date",
    "sentiment",
    "engage_rate",
    "thumbnail_image_url",
    "country",
    "category",
    "ptk_value",
    "language",
    "chart_data",
    "video_safety",
    "video_safety_status",
    "is_content_safe",
    "is_monetizable",
    "description",
    "youtube_published_at",
    "duration",
    "verified",
    "transcript",
    "url",
)

DEFAULT_VIDEO_DETAILS_FIELDS = (
    "video_id",
    "title",
    "channel",
    "views",
    "likes",
    "dislikes",
    "comments",
    "ptk_value",
    "sentiment",
    "engage_rate",
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
    "audience",
    "traffic_sources",
    "duration",
    "verified",
    "video_safety_status",
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
    "channel__top_language",
    "channel__verified",
    "channel__views_per_video",
)


DEFAULT_CHANNEL_LIST_FIELDS = (
    "brand",
    "brand_safety",
    "category",
    "comments",
    "ptk_value",
    "country",
    "description",
    "dislikes",
    "engage_rate",
    "genre",
    "history_date",
    "channel_id",
    "url",
    "is_content_safe",
    "language",
    "top_language",
    "likes",
    "preferred",
    "sentiment",
    "subscribers",
    "social_links",
    "thirty_days_subscribers",
    "thirty_days_views",
    "thumbnail_image_url",
    "title",
    "video_views",
    "video_views_history",
    "videos",
    "views",
    "views_per_video",
    "views_per_video_history",
    "score",
    "score_total",
    "channel_group",
    "verified",
)

DEFAULT_CHANNEL_DETAILS_FIELDS = (
    "channel_id",
    "title",
    "ptk_value",
    "thumbnail_image_url",
    "subscribers",
    "thirty_days_subscribers",
    "thirty_days_views",
    "views_per_video",
    "engage_rate",
    "genre",
    "sentiment",
    "country",
    "category",
    "language",
    "top_language",
    "description",
    "tags",
    "social_stats",
    "social_links",
    "youtube_keywords",
    "chart_data",
    "safety_chart_data",
    "top_keywords",
    "emails",
    "brand",
    "brand_safety",
    "performance",
    "preferred",
    "is_content_safe",
    "youtube_published_at",
    "last_video_published_at",
    "updated_at",
    "score",
    "channel_group",
    "audience",
    "traffic_sources",
    "verified",
)
