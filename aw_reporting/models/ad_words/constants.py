import enum
import logging

from utils.lang import ExtendedEnum
from utils.utils import get_all_class_constants

logger = logging.getLogger(__name__)

BASE_STATS = ("impressions", "video_views", "clicks", "cost")
CLICKS_STATS = (
    "clicks_website",
    "clicks_call_to_action_overlay",
    "clicks_app_store",
    "clicks_cards",
    "clicks_end_cap"
)
CONVERSIONS = ("all_conversions", "conversions", "view_through")

MANAGED_SERVICE_DELIVERY_DATA = (
    "clicks", "cost", "impressions", "video_views"
)

SUM_STATS = BASE_STATS + CONVERSIONS

QUARTILE_RATES = ("quartile_25_rate", "quartile_50_rate",
                  "quartile_75_rate", "quartile_100_rate")

QUARTILE_STATS = ("video_views_25_quartile", "video_views_50_quartile",
                  "video_views_75_quartile", "video_views_100_quartile")

VIEW_RATE_STATS = ("video25rate", "video50rate",
                   "video75rate", "video100rate")


class AgeRange:
    UNDETERMINED = 0
    AGE_18_24 = 1
    AGE_25_34 = 2
    AGE_35_44 = 3
    AGE_45_54 = 4
    AGE_55_64 = 5
    AGE_65_UP = 6


class Gender:
    UNDETERMINED = 0
    FEMALE = 1
    MALE = 2


class Parent:
    PARENT = 0
    NOT_A_PARENT = 1
    UNDETERMINED = 2


class Device:
    _UNDETERMINED = -1
    COMPUTER = 0
    MOBILE = 1
    TABLET = 2
    OTHER = 3
    CONNECTED_TV = 6


ALL_AGE_RANGES = get_all_class_constants(AgeRange)
ALL_GENDERS = get_all_class_constants(Gender)
ALL_PARENTS = get_all_class_constants(Parent)
ALL_DEVICES = get_all_class_constants(Device)

_AGE_RANGE_REPRESENTATION = {
    AgeRange.UNDETERMINED: "Undetermined",
    AgeRange.AGE_18_24: "18-24",
    AgeRange.AGE_25_34: "25-34",
    AgeRange.AGE_35_44: "35-44",
    AgeRange.AGE_45_54: "45-54",
    AgeRange.AGE_55_64: "55-64",
    AgeRange.AGE_65_UP: "65 or more",
}

_GENDER_REPRESENTATION = {
    Gender.UNDETERMINED: "Undetermined",
    Gender.FEMALE: "Female",
    Gender.MALE: "Male",
}

_DEVICE_REPRESENTATION = {
    Device.COMPUTER: "Computers",
    Device.MOBILE: "Mobile devices with full browsers",
    Device.TABLET: "Tablets with full browsers",
    Device.OTHER: "Other",
    Device.CONNECTED_TV: "TV Screens",
}

_PARENT_REPRESENTATION = {
    Parent.PARENT: "Parent",
    Parent.NOT_A_PARENT: "Not a parent",
    Parent.UNDETERMINED: "Undetermined"
}


def age_range_str(age_range_id: int) -> str:
    return _AGE_RANGE_REPRESENTATION.get(age_range_id)


def gender_str(gender_id: int) -> str:
    return _GENDER_REPRESENTATION.get(gender_id)


def device_str(device_id: int) -> str:
    return _DEVICE_REPRESENTATION.get(device_id, "Undetermined")


def parent_str(parent_id):
    return _PARENT_REPRESENTATION.get(parent_id)


class CampaignStatus(ExtendedEnum):
    PAUSED = "paused"
    REMOVED = "removed"
    ELIGIBLE = "eligible"
    ENDED = "ended"
    SERVING = "serving"


DATE_FORMAT = "%Y-%m-%d"
ACTION_STATUSES = ("paused", "removed")

AgeRangeOptions = (
    "AGE_RANGE_UNDETERMINED",
    "AGE_RANGE_18_24",
    "AGE_RANGE_25_34",
    "AGE_RANGE_35_44",
    "AGE_RANGE_45_54",
    "AGE_RANGE_55_64",
    "AGE_RANGE_65_UP",
)

GenderOptions = (
    "GENDER_UNDETERMINED",
    "GENDER_FEMALE",
    "GENDER_MALE",
)

# fixme: remove these tuples
AgeRanges = list(
    age_range_str(age_range_id)
    for age_range_id in ALL_AGE_RANGES
)

Genders = list(
    gender_str(gender_id)
    for gender_id in ALL_GENDERS
)

Devices = list(
    device_str(device_id)
    for device_id in ALL_DEVICES
)


def get_device_id_by_name(device_repr):
    for device_id, device_name in _DEVICE_REPRESENTATION.items():
        if device_repr == device_name:
            return device_id
        if device_repr == "Devices streaming video content to TV screens":
            return Device.CONNECTED_TV
    logger.debug("Undefined device name <%s>", device_repr)
    # pylint: disable=protected-access
    return Device._UNDETERMINED
    # pylint: enable=protected-access


class BudgetType(ExtendedEnum):
    DAILY = "daily"
    TOTAL = "total"


CAMPAIGN_BIDDING_STRATEGY_TYPES = {
    "Target CPA": "cpa",
    "None": None,
}


class BiddingStrategyTypeEnum(enum.Enum):
    """
    bidding strategies enumeration for adgroups and campaigns based on:
    @see https://developers.google.com/adwords/api/docs/appendix/reports/adgroup-performance-report#biddingstrategytype
    This is used w/ adgroup type to create our internal "Product Type"
    """
    MANUAL_CPC = "cpc"
    MANUAL_CPV = "cpv"
    MANUAL_CPM = "cpm"
    PAGE_ONE_PROMOTED = "Target search page location"
    TARGET_SPEND = "Maximize clicks"
    TARGET_CPA = "Target CPA"
    TARGET_ROAS = "Target ROAS"
    MAXIMIZE_CONVERSIONS = "Maximize Conversions"
    MAXIMIZE_CONVERSION_VALUE = "Maximize Conversion Value"
    TARGET_OUTRANK_SHARE = "Target Outranking Share"
    NONE = "None"
    UNKNOWN = "unknown"
    # DOUBLE_DASH = " --"


bidding_strategy_type_id_mapping = {
    BiddingStrategyTypeEnum.MANUAL_CPC.value: 0,
    BiddingStrategyTypeEnum.MANUAL_CPV.value: 1,
    BiddingStrategyTypeEnum.MANUAL_CPM.value: 2,
    BiddingStrategyTypeEnum.PAGE_ONE_PROMOTED.value: 3,
    BiddingStrategyTypeEnum.TARGET_SPEND.value: 4,
    BiddingStrategyTypeEnum.TARGET_CPA.value: 5,
    BiddingStrategyTypeEnum.TARGET_ROAS.value: 6,
    BiddingStrategyTypeEnum.MAXIMIZE_CONVERSIONS.value: 7,
    BiddingStrategyTypeEnum.MAXIMIZE_CONVERSION_VALUE.value: 8,
    BiddingStrategyTypeEnum.TARGET_OUTRANK_SHARE.value: 9,
    BiddingStrategyTypeEnum.UNKNOWN.value: 10,
    BiddingStrategyTypeEnum.NONE.value: None,
    "--": None,
    " --": None,
}


class AdGroupTypeEnum(enum.Enum):
    """
    AdGroupType enum from:
    @see https://developers.google.com/adwords/api/docs/appendix/reports/adgroup-performance-report#adgrouptype
    this is used in combination with BiddingStrategyType to create our internal "ProductType"
    """
    UNKNOWN = "Unknown"
    SEARCH_STANDARD = "Standard"
    SEARCH_DYNAMIC_ADS = "Search Dynamic Ads"
    DISPLAY_STANDARD = "Display"
    DISPLAY_ENGAGEMENT = "Display engagement"
    SHOPPING_PRODUCT_ADS = "Shopping - Product"
    SHOPPING_SHOWCASE_ADS = "Shopping - Showcase"
    SHOPPING_GOAL_OPTIMIZED_ADS = "Shopping - Goal - optimized"
    VIDEO_TRUE_VIEW_IN_STREAM = "In - stream"
    VIDEO_TRUE_VIEW_IN_DISPLAY = "Video discovery"
    VIDEO_BUMPER = "Bumper"
    VIDEO_NON_SKIPPABLE_IN_STREAM = "Standard"
    HOTEL_ADS = "Hotel"


def get_bidding_strategy_type(strategy_type):
    return CAMPAIGN_BIDDING_STRATEGY_TYPES.get(strategy_type, strategy_type)
