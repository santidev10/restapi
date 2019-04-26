from utils.lang import ExtendedEnum


class TargetingType(ExtendedEnum):
    CHANNEL = "channel"
    VIDEO = "video"
    TOPIC = "topic"
    INTEREST = "interest"
    KEYWORD = "keyword"
