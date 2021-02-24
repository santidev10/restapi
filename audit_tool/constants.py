import enum


CHOICE_UNKNOWN_KEY = -1
CHOICE_UNKNOWN_NAME = "Unknown"
CHOICE_UNKNOWN = (CHOICE_UNKNOWN_KEY, CHOICE_UNKNOWN_NAME)


class AuditVideoTranscriptSourceTypeEnum(enum.Enum):
    CUSTOM = 0
    WATSON = 1
    TTS_URL = 2
