import enum

from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import SortDirections
from es_components.constants import VIEWS_FIELD


VETTED_MAPPING = {
    0: "Skipped: Unavailable",
    1: "Skipped: Region",
    2: "Not Suitable",
    3: "Suitable",
    4: None  # Item has not been vetted
}


class SourceListType(enum.Enum):
    INCLUSION = 0
    EXCLUSION = 1


CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY = "custom_segments/featured_images/{uuid}.{extension}"
CUSTOM_SEGMENT_DEFAULT_IMAGE_URL = "https://viewiq-ui-assets.s3.amazonaws.com/common/default_audit_image.png"


class VideoConfig:
    DATA_FIELD = "video"
    AUDIT_TYPE = 1
    SORT_KEY = {VIEWS_FIELD: {"order": SortDirections.DESCENDING}}
    RELATED_STATISTICS_MODEL = YTVideoStatistic
    USER_LIST_SIZE = 20000
    ADMIN_LIST_SIZE = 100000


class ChannelConfig:
    DATA_FIELD = "channel"
    AUDIT_TYPE = 2
    SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
    RELATED_STATISTICS_MODEL = YTChannelStatistic
    USER_LIST_SIZE = 20000
    ADMIN_LIST_SIZE = 100000


class SegmentActionEnum(enum.IntEnum):
    CREATE = 0
    DELETE = 1
    DOWNLOAD = 2


class SegmentTypeEnum(enum.Enum):
    VIDEO = 0
    CHANNEL = 1


class SegmentListType(enum.Enum):
    WHITELIST = 0
    BLACKLIST = 1


class SegmentVettingStatusEnum(enum.Enum):
    NOT_VETTED = 0
    VETTED_SAFE = 1
    VETTED_RISKY = 2


# All current param keys for CustomSegment.params column
class Params:
    HISTORY = "history"

    class VideoExclusion:
        WITH_VIDEO_EXCLUSION = "with_video_exclusion"
        VIDEO_EXCLUSION_FILENAME = "video_exclusion_filename"
        VIDEO_EXCLUSION_SCORE_THRESHOLD = "video_exclusion_score_threshold"

    class GoogleAds:
        GADS_SYNC_DATA = "gads_sync_data"
        CID = "cid"
        AD_GROUP_IDS = "ad_group_ids"

    # DV360
    class DV360:
        DV360_SYNC = "dv360_sync"

    class AuditTool:
        # AuditProcessor PK used to run audit against CTL results
        META_AUDIT_ID = "meta_audit_id"
        INCLUSION_FILE = "inclusion_file"
        EXCLUSION_FILE = "exclusion_file"
        SOURCE_FILE = "source_file"


class Results:
    HISTORY = "history"
    CTL_STATISTICS = "ctl_statistics"
    SYNC = "sync"
    GADS = "gads"
    DV360 = "dv360"
    EXPORT_FILENAME = "export_filename"

