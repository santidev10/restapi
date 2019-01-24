class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"


class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"


class PersistentSegmentTitles:
    MASTER_BLACKLIST_SEGMENT_TITLE = "Master black list"
    MASTER_WHITELIST_SEGMENT_TITLE = "Master white list"

    CATEGORY_MAP = (
        (PersistentSegmentCategory.BLACKLIST, MASTER_BLACKLIST_SEGMENT_TITLE),
        (PersistentSegmentCategory.WHITELIST, MASTER_WHITELIST_SEGMENT_TITLE),
    )
