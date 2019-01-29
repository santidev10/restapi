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
