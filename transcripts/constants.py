import enum

# the iteration of the transcripts processor. Use case: when the processor gets updated, we'll increment this integer
# and use this to query es records to update the processed text.
PROCESSOR_VERSION = 2


class AuditVideoTranscriptSourceTypeIdEnum(enum.Enum):
    """
    Used for PG AuditVideoTranscript's Source type
    """
    CUSTOM = 0
    WATSON = 1
    TTS_URL = 2


class TranscriptSourceTypeEnum(enum.Enum):
    """
    Used for ES Transcript's source_type
    """
    CUSTOM = "timedtext"
    WATSON = "Watson"
    TTS_URL = "tts_url"


source_type_by_id = {
    AuditVideoTranscriptSourceTypeIdEnum.CUSTOM.value: TranscriptSourceTypeEnum.CUSTOM.value,
    AuditVideoTranscriptSourceTypeIdEnum.WATSON.value: TranscriptSourceTypeEnum.WATSON.value,
    AuditVideoTranscriptSourceTypeIdEnum.TTS_URL.value: TranscriptSourceTypeEnum.TTS_URL.value,
}
