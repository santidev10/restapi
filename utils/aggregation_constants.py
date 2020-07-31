ALLOWED_CHANNEL_AGGREGATIONS = (
    "ads_stats.average_cpv:max",
    "ads_stats.average_cpv:min",
    "ads_stats.average_cpm:max",
    "ads_stats.average_cpm:min",
    "ads_stats.ctr:max",
    "ads_stats.ctr:min",
    "ads_stats.ctr_v:max",
    "ads_stats.ctr_v:min",
    "ads_stats:exists",
    "ads_stats.video_view_rate:max",
    "ads_stats.video_view_rate:min",
    "ads_stats.video_quartile_100_rate:max",
    "ads_stats.video_quartile_100_rate:min",
    "analytics.age13_17:max",
    "analytics.age13_17:min",
    "analytics.age18_24:max",
    "analytics.age18_24:min",
    "analytics.age25_34:max",
    "analytics.age25_34:min",
    "analytics.age35_44:max",
    "analytics.age35_44:min",
    "analytics.age45_54:max",
    "analytics.age45_54:min",
    "analytics.age55_64:max",
    "analytics.age55_64:min",
    "analytics.age65_:max",
    "analytics.age65_:min",
    "analytics.gender_female:max",
    "analytics.gender_female:min",
    "analytics.gender_male:max",
    "analytics.gender_male:min",
    "analytics.gender_other:max",
    "analytics.gender_other:min",
    "brand_safety",
    "brand_safety.limbo_status",
    "custom_properties.is_tracked",
    "custom_properties.preferred",
    "general_data.country_code",
    "general_data.top_category",
    "general_data.top_lang_code",
    "general_data.iab_categories",
    "monetization.is_monetizable:exists",
    "stats.last_30day_subscribers:max",
    "stats.last_30day_subscribers:min",
    "stats.last_30day_views:max",
    "stats.last_30day_views:min",
    "stats.subscribers:max",
    "stats.subscribers:min",
    "stats.views_per_video:max",
    "stats.views_per_video:min",
    "stats.views_per_video:min",
    "task_us_data.age_group",
    "task_us_data.content_quality",
    "task_us_data.content_type",
    "task_us_data.gender",
    "task_us_data:exists",
    "task_us_data:missing",
)

ALLOWED_VIDEO_AGGREGATIONS = (
    "ads_stats.average_cpv:max",
    "ads_stats.average_cpv:min",
    "ads_stats.average_cpm:max",
    "ads_stats.average_cpm:min",
    "ads_stats.ctr:max",
    "ads_stats.ctr:min",
    "ads_stats.ctr_v:max",
    "ads_stats.ctr_v:min",
    "ads_stats.video_view_rate:max",
    "ads_stats.video_view_rate:min",
    "ads_stats.video_quartile_100_rate:max",
    "ads_stats.video_quartile_100_rate:min",
    "brand_safety",
    "brand_safety.limbo_status",
    "general_data.category",
    "general_data.country_code",
    "general_data.lang_code",
    "general_data.youtube_published_at:max",
    "general_data.youtube_published_at:min",
    "general_data.iab_categories",
    "stats.flags:exists",
    "stats.flags:missing",
    "stats.channel_subscribers:max",
    "stats.channel_subscribers:min",
    "stats.last_day_views:max",
    "stats.last_day_views:min",
    "stats.views:max",
    "stats.views:min",
    "stats.flags",
    "custom_captions.items:exists",
    "custom_captions.items:missing",
    "captions:exists",
    "captions:missing",
    "stats.sentiment",
    "task_us_data.age_group",
    "task_us_data.content_quality",
    "task_us_data.content_type",
    "task_us_data.gender",
    "task_us_data:exists",
    "task_us_data:missing",
    "transcripts:exists",
    "transcripts:missing",
    "flags",
    "monetization.is_monetizable:exists",
    "ads_stats:exists"
)

ALLOWED_KEYWORD_AGGREGATIONS = (
    "stats.search_volume:min",
    "stats.search_volume:max",
    "stats.average_cpc:min",
    "stats.average_cpc:max",
    "stats.competition:min",
    "stats.competition:max",
    "stats.is_viral",
)
