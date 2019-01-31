S3_SEGMENT_EXPORT_KEY_PATTERN = "persistent-segments/{segment_type}/{segment_title}.csv"


class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"


class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"


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

    CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, CHANNEL_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, CHANNEL_WHITELIST_CSV_COLUMNS),
    )

    VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, VIDEO_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, VIDEO_WHITELIST_CSV_COLUMNS),
    )

    CSV_COLUMNS_MAPS_BY_TYPE = (
        (PersistentSegmentType.CHANNEL, CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY),
        (PersistentSegmentType.VIDEO, VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY),
    )
