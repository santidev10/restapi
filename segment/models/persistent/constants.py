class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"


class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"


class PersistentSegmentTitles:
    MASTER_BLACKLIST_SEGMENT_TITLE = "Master Blacklist"
    MASTER_WHITELIST_SEGMENT_TITLE = "Master Whitelist"

    CATEGORY_MAP = (
        (PersistentSegmentCategory.BLACKLIST, MASTER_BLACKLIST_SEGMENT_TITLE),
        (PersistentSegmentCategory.WHITELIST, MASTER_WHITELIST_SEGMENT_TITLE),
    )
