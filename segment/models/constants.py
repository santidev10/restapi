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
    LIST_SIZE = 100000


class ChannelConfig:
    DATA_FIELD = "channel"
    AUDIT_TYPE = 2
    SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
    RELATED_STATISTICS_MODEL = YTChannelStatistic
    LIST_SIZE = 100000
