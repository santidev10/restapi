from django.conf import settings

GET_DF = "%Y-%m-%d"
MIN_FETCH_DATE = settings.MIN_AW_FETCH_DATE

# Mapping of google ads api response objects to CHF models
YOUTUBE_CHANNEL = "YOUTUBE_CHANNEL"
YOUTUBE_VIDEO = "YOUTUBE_VIDEO"
DATE_YMD = "%Y-%m-%d"
PARENT_STATUSES = ('parent', 'not_a_parent', 'undetermined')

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
    "customer_client": ("client_customer", "currency_code", "descriptive_name", "id", "hidden", "manager", "test_account", "time_zone"),
}

CUSTOMER_DETAILS_FIELDS = {
    "customer": ("descriptive_name", "currency_code", "time_zone", "test_account")
}

CUSTOMER_CLIENT_LINK_FIELDS = {
    "customer_client_link": ("client_customer", "manager_link_id", "status", "hidden")
}

AD_PERFORMANCE_FIELDS = {
    "ad_group": ("id",),
    "ad_group_ad": ("ad.id", "ad.text_ad.headline", "ad.image_ad.name", "ad.display_url", "status", "policy_summary"),
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
    "ad_group": ("id", "name", "status", "type"),
    "metrics": ("active_view_impressions", "engagements") + COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
    "segments": ("ad_network_type", "date", "device")
}

AUDIENCE_PERFORMANCE_FIELDS = {
    "user_list": {
        "user_list": ("id", "name",)
    },
    "performance": {
        "ad_group": ("id",),
        "ad_group_criterion": ("criterion_id", "user_list.user_list", "type", "user_interest.user_interest_category", "custom_affinity.custom_affinity"),
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
    "ad_group": ("id",),
    "ad_group_criterion": ("placement.url", "type", "youtube_channel.channel_id", "youtube_video.video_id"),
    "metrics": COMPLETED_FIELDS + MAIN_STATISTICS_FIELDS,
    "segments": ("date", "device"),
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
