from django.conf import settings
from google.ads.google_ads.v2.services.enums import AdNetworkTypeEnum
from google.ads.google_ads.v2.services.enums import AgeRangeTypeEnum
from google.ads.google_ads.v2.services.enums import DeviceEnum
from google.ads.google_ads.v2.services.enums import GenderTypeEnum
from google.ads.google_ads.v2.services.enums import ParentalStatusTypeEnum

from aw_reporting.models.ad_words.constants import AgeRange
from aw_reporting.models.ad_words.constants import Device
from aw_reporting.models.ad_words.constants import Gender
from aw_reporting.models.ad_words.constants import Parent

GET_DF = "%Y-%m-%d"
MIN_FETCH_DATE = settings.MIN_AW_FETCH_DATE
CLICKS_MODEL_UPDATE_FIELDS = (
    "clicks_website", "clicks_call_to_action_overlay", "clicks_app_store", "clicks_cards", "clicks_end_cap")
BASE_STATISTIC_MODEL_UPDATE_FIELDS = (
    "impressions", "video_views", "clicks", "cost", "conversions", "all_conversions", "view_through",
    "video_views_25_quartile", "video_views_50_quartile", "video_views_75_quartile", "video_views_100_quartile")
STATS_MODELS_COMBINED_UPDATE_FIELDS = CLICKS_MODEL_UPDATE_FIELDS + BASE_STATISTIC_MODEL_UPDATE_FIELDS

# Mapping of google ads api response objects to CHF models
YOUTUBE_CHANNEL = "YOUTUBE_CHANNEL"
YOUTUBE_VIDEO = "YOUTUBE_VIDEO"
DATE_YMD = "%Y-%m-%d"
PARENT_STATUSES = ("parent", "not_a_parent", "undetermined")

MAIN_STATISTICS_FIELDS = (
    "video_views", "cost_micros", "clicks", "impressions", "conversions", "all_conversions", "view_through_conversions"
)

STATISTICS_FIELDS = ("video_view_rate", "ctr", "average_cpv", "average_cpm") + MAIN_STATISTICS_FIELDS

COMPLETED_FIELDS = (
    "video_quartile_25_rate", "video_quartile_50_rate",
    "video_quartile_75_rate", "video_quartile_100_rate"
)

DAILY_STATISTIC_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "metrics": MAIN_STATISTICS_FIELDS + COMPLETED_FIELDS,
    "segments": ("date",),
}

CUSTOMER_CLIENT_ACCOUNT_FIELDS = {
    "customer_client": (
        "client_customer", "currency_code", "descriptive_name", "id", "hidden", "manager", "test_account", "time_zone"),
}

CUSTOMER_DETAILS_FIELDS = {
    "customer": ("descriptive_name", "currency_code", "time_zone", "test_account")
}

CUSTOMER_CLIENT_LINK_FIELDS = {
    "customer_client_link": ("client_customer", "manager_link_id", "status", "hidden")
}

AD_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "ad_group_ad": ("ad.id", "ad.text_ad.headline", "ad.name", "ad.display_url", "status", "policy_summary"),
    "metrics": COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
    "segments": ("date",)
}

CAMPAIGN_GENERAL_PERFORMANCE_FIELDS = {
    "campaign": ("id", "name", "status", "serving_status", "start_date", "end_date", "advertising_channel_type"),
    "campaign_budget": ("amount_micros", "total_amount_micros"),
}

CAMPAIGN_PERFORMANCE_FIELDS = {
    **CAMPAIGN_GENERAL_PERFORMANCE_FIELDS,
    "segments": ("device", "date"),
    "metrics": ("clicks", "impressions") + COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS
}

CAMPAIGN_HOURLY_PERFORMANCE_FIELDS = {
    **CAMPAIGN_GENERAL_PERFORMANCE_FIELDS,
    "segments": ("date", "hour"),
    "metrics": ("video_views", "cost_micros", "clicks", "impressions", "conversions")
}

CLICKS_PERFORMANCE_FIELDS = {
    "segments": ("click_type", "date"),
    "metrics": ("clicks",)
}

AD_GROUP_PERFORMANCE_FIELDS = {
    "campaign": ("id",),
    "ad_group": ("id", "name", "status", "type", "cpc_bid_micros", "cpm_bid_micros", "cpv_bid_micros"),
    "metrics": ("active_view_impressions", "engagements") + COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
    "segments": ("ad_network_type", "date", "device")
}

AUDIENCE_PERFORMANCE_FIELDS = {
    "user_list": {
        "user_list": ("id", "name",)
    },
    "performance": {
        "ad_group": ("id",),
        "ad_group_criterion": ("criterion_id", "user_list.user_list", "type", "user_interest.user_interest_category",
                               "custom_affinity.custom_affinity", "custom_intent.custom_intent"),
        "metrics": COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
        "segments": ("date",),
    }
}

CAMPAIGN_HOURLY_STATS_PERFORMANCE_FIELDS = {
    "campaign": ("advertising_channel_type", "id", "name", "serving_status", "status", "start_date", "end_date"),
    "campaign_budget": ("amount_micros",),
    "metrics": ("video_views", "cost_micros", "clicks", "impressions", "conversions", "view_through_conversions"),
    "segments": ("hour", "date"),
}

CAMPAIGN_LOCATION_PERFORMANCE_FIELDS = {
    "campaign": ("id", "name", "end_date", "start_date"),
    "campaign_criterion": ("criterion_id", "location_group", "negative", "location.geo_target_constant"),
    "metrics": MAIN_STATISTICS_FIELDS,
    "segments": ("date",),
}

CITY_MAIN_METRICS_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "campaign": ("id",),
    "metrics": MAIN_STATISTICS_FIELDS,
    "segments": ("date", "geo_target_city"),
}

CITY_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "campaign": ("id",),
    "metrics": ("cost_micros",),
    "segments": ("geo_target_city",),
    "geographic_view": ("country_criterion_id", "location_type"),
}

GEO_TARGET_CONSTANT_FIELDS = {
    "geo_target_constant": ("id", "name", "country_code", "target_type", "canonical_name")
}

KEYWORD_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "ad_group_criterion": ("criterion_id", "keyword.text"),
    "metrics": MAIN_STATISTICS_FIELDS + COMPLETED_FIELDS,
    "segments": ("date",),
}

PARENT_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "ad_group_criterion": ("criterion_id", "parental_status.type"),
    "metrics": MAIN_STATISTICS_FIELDS + COMPLETED_FIELDS,
    "segments": ("date",),
}

PLACEMENT_PERFORMANCE_FIELDS = {
    "managed": {
        "ad_group": ("id",),
        "ad_group_criterion": ("placement.url", "type", "youtube_channel.channel_id", "youtube_video.video_id"),
        "metrics": COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
        "segments": ("date", "device"),
    },
    "group": {
        "ad_group": ("id",),
        "group_placement_view": ("display_name", "placement", "placement_type", "target_url"),
        "metrics": MAIN_STATISTICS_FIELDS,
        "segments": ("date", "device"),
    }
}

TOPIC_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "ad_group_criterion": ("criterion_id", "topic.path", "topic.topic_constant"),
    "metrics": COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
    "segments": ("date",),
}

VIDEO_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "video": ("channel_id", "duration_millis", "id"),
    "metrics": tuple(set(MAIN_STATISTICS_FIELDS) - {"all_conversions"}) + COMPLETED_FIELDS,
    "segments": ("date",),
}

# Mappings of Google Ads enum values to object ids throughout application
GENDER_ENUM_TO_ID = {
    GenderTypeEnum.GenderType.UNDETERMINED: Gender.UNDETERMINED,
    GenderTypeEnum.GenderType.FEMALE: Gender.FEMALE,
    GenderTypeEnum.GenderType.MALE: Gender.MALE,
}

AGE_RANGE_ENUM_TO_ID = {
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_UNDETERMINED: AgeRange.UNDETERMINED,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_18_24: AgeRange.AGE_18_24,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_25_34: AgeRange.AGE_25_34,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_35_44: AgeRange.AGE_35_44,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_45_54: AgeRange.AGE_45_54,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_55_64: AgeRange.AGE_55_64,
    AgeRangeTypeEnum.AgeRangeType.AGE_RANGE_65_UP: AgeRange.AGE_65_UP,
}

DEVICE_ENUM_TO_ID = {
    # pylint: disable=protected-access
    DeviceEnum.Device.UNKNOWN: Device._UNDETERMINED,
    # pylint: enable=protected-access
    DeviceEnum.Device.DESKTOP: Device.COMPUTER,
    DeviceEnum.Device.MOBILE: Device.MOBILE,
    DeviceEnum.Device.TABLET: Device.TABLET,
    DeviceEnum.Device.OTHER: Device.OTHER,
    DeviceEnum.Device.CONNECTED_TV: Device.CONNECTED_TV,
}

PARENT_ENUM_TO_ID = {
    ParentalStatusTypeEnum.ParentalStatusType.PARENT: Parent.PARENT,
    ParentalStatusTypeEnum.ParentalStatusType.NOT_A_PARENT: Parent.NOT_A_PARENT,
    ParentalStatusTypeEnum.ParentalStatusType.UNDETERMINED: Parent.UNDETERMINED,
}

AD_NETWORK_ENUM_TO_STR = {
    AdNetworkTypeEnum.AdNetworkType.UNSPECIFIED: "unspecified",
    AdNetworkTypeEnum.AdNetworkType.UNKNOWN: "unknown",
    AdNetworkTypeEnum.AdNetworkType.SEARCH: "Search Network",
    AdNetworkTypeEnum.AdNetworkType.SEARCH_PARTNERS: "Search Partners",
    AdNetworkTypeEnum.AdNetworkType.CONTENT: "Display Network",
    AdNetworkTypeEnum.AdNetworkType.YOUTUBE_SEARCH: "YouTube Search",
    AdNetworkTypeEnum.AdNetworkType.YOUTUBE_WATCH: "YouTube Videos",
    AdNetworkTypeEnum.AdNetworkType.MIXED: "Cross-network"
}
