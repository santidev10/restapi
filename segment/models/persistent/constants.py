class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"

    ALL_OPTIONS = (
        (BLACKLIST, BLACKLIST),
        (WHITELIST, WHITELIST),
    )


class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"
