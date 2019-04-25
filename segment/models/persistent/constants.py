S3_SEGMENT_EXPORT_KEY_PATTERN = "persistent-segments/{segment_type}/{segment_title}.csv"
S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL = "https://s3.amazonaws.com/viewiq-prod/persistent-segments/thumbnails/cf-default.jpg"

class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"


class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"
    TOPIC = "topic"


class PersistentSegmentTitles:
    CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE = "Channels Master Blacklist"
    CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE = "Channels Master Whitelist"

    VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE = "Videos Master Blacklist"
    VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE = "Videos Master Whitelist"

    ALL_MASTER_SEGMENT_TITLES = (
        CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE,
        CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE,
    )

    MASTER_BLACKLIST_SEGMENT_TITLES = (
        CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE,
    )

    MASTER_WHITELIST_SEGMENT_TITLES = (
        CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE,
    )

    TITLES_MAP = (
        (PersistentSegmentType.CHANNEL, (
            (PersistentSegmentCategory.BLACKLIST, CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE),
            (PersistentSegmentCategory.WHITELIST, CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE),
        )),
        (PersistentSegmentType.VIDEO, (
            (PersistentSegmentCategory.BLACKLIST, VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE),
            (PersistentSegmentCategory.WHITELIST, VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE),
        )),
    )

    CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE = "Curated Channels Master Whitelist"

    NO_AUDIT_SEGMENTS = (
        CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
    )


class PersistentSegmentExportColumn:
    URL = "URL"
    TITLE = "Title"
    CATEGORY = "Category"
    LANGUAGE = "Language"
    THUMBNAIL = "Thumbnail"
    LIKES = "Likes"
    DISLIKES = "Dislikes"
    VIEWS = "Views"
    SUBSCRIBERS = "Subscribers"
    AUDITED_VIDEOS = "Audited Videos"
    BAD_WORDS = "Bad Words"
    CHANNEL_ID = "Channel ID"
    CHANNEL_TITLE = "Channel Title"

    CHANNEL_BLACKLIST_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LANGUAGE,
        THUMBNAIL,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
        BAD_WORDS,
    )

    CHANNEL_WHITELIST_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
    )

    CHANNEL_TOPIC_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
        BAD_WORDS,
    )

    VIDEO_BLACKLIST_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LANGUAGE,
        THUMBNAIL,
        LIKES,
        DISLIKES,
        VIEWS,
        BAD_WORDS,
    )

    VIDEO_WHITELIST_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LIKES,
        DISLIKES,
        VIEWS,
    )

    VIDEO_TOPIC_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LIKES,
        DISLIKES,
        VIEWS,
        BAD_WORDS,
        CHANNEL_ID,
        CHANNEL_TITLE
    )

    CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, CHANNEL_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, CHANNEL_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.TOPIC, CHANNEL_TOPIC_CSV_COLUMNS),
    )

    VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, VIDEO_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, VIDEO_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.TOPIC, VIDEO_TOPIC_CSV_COLUMNS),
    )

    CSV_COLUMNS_MAPS_BY_TYPE = (
        (PersistentSegmentType.CHANNEL, CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY),
        (PersistentSegmentType.VIDEO, VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY),
    )

PERSISTENT_SEGMENT_CHANNEL_PREVIEW_FIELDS = (
    "channel_id",
    "brand_safety",
    "category",
    "country",
    "url",
    "language",
    "preferred",
    "subscribers",
    "thirty_days_subscribers",
    "daily_subscribers",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "thumbnail_image_url",
    "title",
    "video_views",
    "videos",
    "views",
    "views_per_video",
    "verified",
)

PERSISTENT_SEGMENT_VIDEO_PREVIEW_FIELDS = (
    "video_id",
    "title",
    "views",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "comments",
    "likes",
    "thirty_days_likes",
    "weekly_likes",
    "daily_likes",
    "dislikes",
    "thumbnail_image_url",
    "country",
    "category",
    "language",
    "is_flagged",
    "duration",
    "verified",
    "url",
    "license",
    "bad_statuses",
    "title_bad_words",
    "title_bad_statuses",
)
