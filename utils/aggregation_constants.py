ALLOWED_CHANNEL_AGGREGATIONS = (
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "ads_stats:exists",
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
        "general_data.emails:exists",
        "general_data.emails:missing",
        "custom_properties.preferred",
        "custom_properties.is_tracked",
        "general_data.country",
        "general_data.country_code",
        "general_data.top_category",
        "general_data.top_language",
        "general_data.top_lang_code",
        "general_data.iab_categories",
        "social.facebook_likes:max",
        "social.facebook_likes:min",
        "social.instagram_followers:max",
        "social.instagram_followers:min",
        "social.twitter_followers:max",
        "social.twitter_followers:min",
        "stats.last_30day_subscribers:max",
        "stats.last_30day_subscribers:min",
        "stats.last_30day_views:max",
        "stats.last_30day_views:min",
        "stats.subscribers:max",
        "stats.subscribers:min",
        "stats.views_per_video:max",
        "stats.views_per_video:min",
        "task_us_data.age_group",
        "task_us_data.content_type",
        "task_us_data.gender",
        "brand_safety",
        "stats.channel_group",
        "monetization.is_monetizable:exists",
    )

ALLOWED_VIDEO_AGGREGATIONS = (
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "general_data.category",
        "general_data.country",
        "general_data.country_code",
        "general_data.language",
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
        "brand_safety",
        "stats.flags",
        "custom_captions.items:exists",
        "custom_captions.items:missing",
        "captions:exists",
        "captions:missing",
        "stats.sentiment:max",
        "stats.sentiment:min",
        "transcripts:exists",
        "transcripts:missing",
        "flags"
    )

ALLOWED_KEYWORD_AGGREGATIONS = (
        "stats.search_volume:min",
        "stats.search_volume:max",
        "stats.average_cpc:min",
        "stats.average_cpc:max",
        "stats.competition:min",
        "stats.competition:max",
        "stats.is_viral"
    )

