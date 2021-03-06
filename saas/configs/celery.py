import os
import re
from datetime import timedelta

from celery.schedules import crontab

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_API_PORT = os.getenv("RABBITMQ_API_PORT", "15672")
RABBITMQ_AMQP_PORT = os.getenv("RABBITMQ_AMQP_PORT", "5672")

RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

CELERY_ACCEPT_CONTENT = ["celery_result", "json"]
CELERY_RESULT_SERIALIZER = "celery_result"

RABBITMQ_API_URL = "{host}:{port}".format(host=RABBITMQ_HOST, port=RABBITMQ_API_PORT)
DEFAULT_CELERY_BROKER_URL = "amqp://{user}:{password}@{host}:{port}".format(
    user=RABBITMQ_USER, password=RABBITMQ_PASSWORD, host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)
CELERY_BROKER_URL = "{broker_url}/restapi".format(broker_url=DEFAULT_CELERY_BROKER_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "elasticsearch://es:9200/celery/task_result")
CELERY_RESULT_EXTENDED = True
CELERY_ELASTICSEARCH_TIMEOUT = int(os.getenv("CELERY_ELASTICSEARCH_TIMEOUT", "600"))

DMP_CELERY_BROKER_URL = "{broker_url}/dmp".format(broker_url=DEFAULT_CELERY_BROKER_URL)
DMP_CELERY_RESULT_BACKEND = os.getenv("DMP_RESULT_BACKEND", CELERY_RESULT_BACKEND)
DMP_CELERY_RESULT_EXTENDED = True
DMP_CELERY_ELASTICSEARCH_TIMEOUT = int(os.getenv("DMP_CELERY_ELASTICSEARCH_TIMEOUT", "600"))
DMP_CELERY_ACCEPT_CONTENT = ["celery_result", "json"]
DMP_CELERY_RESULT_SERIALIZER = "celery_result"

CELERY_TIMEZONE = "UTC"


CELERY_BEAT_SCHEDULE = {
    "google_ads_campaign_update": {
        "task": "aw_reporting.google_ads.tasks.update_campaigns.setup_update_campaigns",
        "schedule": crontab(hour="*", minute="*/5"),
    },
    "google_ads_update_without_campaigns": {
        "task": "aw_reporting.google_ads.tasks.update_without_campaigns.setup_update_without_campaigns",
        "schedule": crontab(hour="*", minute="*/5"),
    },
    "google_ads_update_audiences": {
        "task": "aw_reporting.google_ads.tasks.update_audiences.update_audiences",
        "schedule": crontab(day_of_month="1", hour="0", minute="0"),
    },
    "google_ads_update_geo_targets": {
        "task": "aw_reporting.google_ads.tasks.update_geo_targets.update_geo_targets",
        "schedule": crontab(day_of_month="1", hour="1", minute="0"),
    },
    "google_ads_geo_view_ad_group_stats": {
        "task": "aw_reporting.google_ads.tasks.update_geo_view_ad_group_stats.update_geo_view_ad_group_stats_task",
        "schedule": crontab(hour="*", minute="*/5"),
    },
    "full-sf-update": {
        "task": "aw_reporting.update.update_salesforce_data.update_salesforce_data",
        "schedule": crontab(hour="*", minute="0"),
        "kwargs": dict(do_update=os.getenv("DO_SALESFORCE_UPDATE", "0") == "1")
    },
    "schedule_daily_email_notifications": {
        "task": "email_reports.tasks.schedule_daily_reports",
        "schedule": crontab(hour="0", minute="0"),
        "kwargs": dict(
            reports=["CampaignUnderMargin", "TechFeeCapExceeded", "CampaignUnderPacing",
                     "CampaignOverPacing", "FlightDeliveredReport"],
        ),
    },
    "daily_es_monitoring_report": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30"),
        "kwargs": dict(
            reports=["ESMonitoringEmailReport"],
        ),
    },
    "daily_apex_notifications": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30"),
        "kwargs": dict(
            reports=["DailyApexVisaCampaignEmailReport", "DailyApexDisneyCampaignEmailReport"],
        ),
    },
    "recreate-demo-data": {
        "task": "aw_reporting.demo.recreate_demo_data.recreate_demo_data",
        "schedule": crontab(hour="0", minute="0"),
    },
    "update-videos-percentiles": {
        "task": "video.tasks.update_videos_percentiles.update_videos_percentiles",
        "schedule": 3600,
    },
    "update-channels-percentiles": {
        "task": "channel.tasks.update_channels_percentiles.update_channels_percentiles",
        "schedule": 3600,
    },
    "update-keywords-percentiles": {
        "task": "keywords.tasks.update_keywords_percentiles.update_keywords_percentiles",
        "schedule": 3600,
    },
    "pull-custom-transcripts": {
        "task": "transcripts.tasks.pull_custom_transcripts.pull_custom_transcripts",
        "schedule": 90
    },
    "cache-video-aggregations": {
        "task": "cache.tasks.cache_video_aggregations.cache_video_aggregations",
        "schedule": crontab(hour="*", minute="*/30"),
    },
    "cache-channel-aggregations": {
        "task": "cache.tasks.cache_channel_aggregations.cache_channel_aggregations",
        "schedule": crontab(hour="*", minute="*/30"),
    },
    "cache_research_defaults": {
        "task": "cache.tasks.cache_research_defaults.cache_research_defaults_task",
        "schedule": crontab(hour="*", minute="*/30"),
    },
    "cache_pricing_tool_filters": {
        "task": "cache.tasks.cache_pricing_tool_filters.cache_pricing_tool_filters",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "cache_global_trends_filters": {
        "task": "cache.tasks.cache_global_trends_filters.cache_global_trends_filters",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "cache_forecast_tool_filters": {
        "task": "cache.tasks.cache_forecast_tool_filters.cache_forecast_tool_filters",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "cache_industry_performance": {
        "task": "cache.tasks.cache_industry_performance.cache_industry_performance",
        "schedule": crontab(hour="*", minute="*/30"),
    },
    "cache_pacing_report_filters": {
        "task": "cache.tasks.cache_pacing_report_filters.cache_pacing_report_filters",
        "schedule": crontab(minute=0, hour="*/2"),
    },
    "brand_safety_channel_discovery": {
        "task": "brand_safety.tasks.channel_discovery.channel_discovery_scheduler",
        "schedule": 60 * 2,
    },
    "brand_safety_channel_outdated": {
        "task": "brand_safety.tasks.channel_outdated.channel_outdated_scheduler",
        "schedule": 60 * 2,
    },
    "brand_safety_video_discovery": {
        "task": "brand_safety.tasks.video_discovery.video_discovery_scheduler",
        "schedule": 60 * 2,
    },
    "userprofile_clean_device_auth_tokens": {
        "task": "userprofile.tasks.clean_device_auth_tokens.clean_device_auth_tokens",
        "schedule": crontab(day_of_month="1", hour="1", minute="0"),
    },
    "segment_update_statistics": {
        "task": "segment.tasks.update_segment_statistics.update_segment_statistics",
        "schedule": crontab(minute="*/10"),
    },
    "regenerate_custom_segments": {
        "task": "segment.tasks.regenerate_custom_segments.regenerate_custom_segments_with_lock",
        "schedule": crontab(minute="*/10"),
    },
    "update_account_performance": {
        "task": "dashboard.tasks.update_account_performance.update_account_performance_task",
        "schedule": crontab(minute="0", hour="23"),
    },
    "update_opportunities_stats": {
        "task": "aw_reporting.update.update_opportunities.update_opportunities_task",
        "schedule": crontab(minute="0", hour="*"),
    },
    "daily_ingest_ias_channels": {
        "task": "channel.tasks.ingest_ias_channels_v2.ingest_ias_channels",
        # defaults to twice per day, 0500, 1700 PST
        "schedule": crontab(hour=os.getenv("IAS_INGESTION_SCHEDULE_HOUR", "0,12"), minute=0),
    },
    "sync_dv360": {
        "task": "oauth.tasks.dv360.sync_dv_records.sync_dv360",
        "schedule": crontab(minute="*/10"),
    },
    "oauth_google_ads_update": {
        "task": "oauth.tasks.google_ads_update.google_ads_update_task",
        "schedule": crontab(minute="*/10")
    },
    "oauth_gads_notify": {
        "task": "oauth.tasks.segment_gads_oauth_notify.segment_gads_oauth_notify_task",
        "schedule": crontab(minute="*/10")
    }
}

class Queue:
    DEFAULT = "celery"
    REPORTS = "reports"
    EXPORT = "export"
    SEGMENTS = "segments"
    DELIVERY_STATISTIC_UPDATE = "delivery_statistic"
    EMAIL_REPORTS = "email_reports"
    HOURLY_STATISTIC = "hourly_statistic"
    TRANSCRIPTS = "transcripts"
    CACHE_RESEARCH = "cache_research"
    BRAND_SAFETY_CHANNEL_LIGHT = "brand_safety_channel_light"
    BRAND_SAFETY_CHANNEL_PRIORITY = "brand_safety_channel_priority"
    BRAND_SAFETY_VIDEO_PRIORITY = "brand_safety_video_priority"
    SCHEDULERS = "schedulers"
    IAS = "ias"
    PERFORMIQ = "performiq"
    CTL_VIDEO_EXCLUSION = "ctl_video_exclusion"
    OAUTH = "oauth"


CELERY_ROUTES_PREPARED = [
    ("transcripts.tasks.*", {"queue": Queue.TRANSCRIPTS}),
    ("aw_reporting.google_ads.tasks.update_campaigns.*", {"queue": Queue.HOURLY_STATISTIC}),
    ("aw_reporting.google_ads.tasks.update_without_campaigns.*", {"queue": Queue.DELIVERY_STATISTIC_UPDATE}),
    ("aw_reporting.update.*", {"queue": Queue.HOURLY_STATISTIC}),
    ("aw_reporting.google_ads.tasks.update_geo_view_ad_group_stats.*", {"queue": Queue.HOURLY_STATISTIC}),
    ("aw_reporting.reports.*", {"queue": Queue.REPORTS}),
    ("cache.tasks.*", {"queue": Queue.CACHE_RESEARCH}),
    ("email_reports.*", {"queue": Queue.EMAIL_REPORTS}),
    ("*export*", {"queue": Queue.EXPORT}),
    ("segment.tasks.*segment*", {"queue": Queue.SEGMENTS}),
    ("*_scheduler", {"queue": Queue.SCHEDULERS}),
    ("channel.tasks.ingest_ias_channels_v2.*", {"queue": Queue.IAS}),
    ("performiq.tasks.*", {"queue": Queue.PERFORMIQ}),
    ("oauth.tasks.*", {"queue": Queue.OAUTH}),
    ("segment.tasks.generate_video_exclusion.generate_video_exclusion", {"queue": Queue.CTL_VIDEO_EXCLUSION}),
    ("*", {"queue": Queue.DEFAULT}),
]
# dirty fix for celery. fixes AttributeError
# pylint: disable=protected-access
re._pattern_type = re.Pattern
# pylint: enable=protected-access

CELERY_TASK_ROUTES = (CELERY_ROUTES_PREPARED,)


class TaskExpiration:
    FULL_AW_UPDATE = timedelta(hours=8).total_seconds()
    FULL_AW_ACCOUNT_UPDATE = timedelta(hours=1).total_seconds()
    HOURLY_AW_UPDATE = timedelta(hours=1).total_seconds()
    FULL_SF_UPDATE = timedelta(hours=1).total_seconds()
    CUSTOM_TRANSCRIPTS = timedelta(minutes=10).total_seconds()
    TTS_URL_TRANSCRIPTS = timedelta(minutes=4).total_seconds()
    BRAND_SAFETY_CHANNEL_DISCOVERY = timedelta(hours=2).total_seconds()
    BRAND_SAFETY_CHANNEL_OUTDATED = timedelta(hours=2).total_seconds()
    BRAND_SAFETY_VIDEO_DISCOVERY = timedelta(hours=2).total_seconds()
    RESEARCH_CACHING = timedelta(minutes=30).total_seconds()
    PRICING_TOOL_FILTERS_CACHING = timedelta(hours=3).total_seconds()
    GLOBAL_TRENDS_FILTERS_CACHING = timedelta(hours=3).total_seconds()
    FORECAST_TOOL_FILTERS_CACHING = timedelta(hours=3).total_seconds()
    INDUSTRY_PERFORMANCE_CACHING = timedelta(minutes=30).total_seconds()
    PACING_REPORT_FILTERS = timedelta(hours=2).total_seconds()
    INGEST_IAS = timedelta(hours=11).total_seconds()


class TaskTimeout:
    FULL_AW_UPDATE = timedelta(hours=8).total_seconds()
    FULL_AW_ACCOUNT_UPDATE = timedelta(hours=1).total_seconds()
    HOURLY_AW_UPDATE = timedelta(hours=1).total_seconds()
    FULL_SF_UPDATE = timedelta(hours=1).total_seconds()
    CUSTOM_TRANSCRIPTS = timedelta(minutes=30).total_seconds()
    TTS_URL_TRANSCRIPTS = timedelta(minutes=8).total_seconds()
    BRAND_SAFETY_CHANNEL_DISCOVERY = timedelta(minutes=30).total_seconds()
    BRAND_SAFETY_CHANNEL_OUTDATED = timedelta(hours=2).total_seconds()
    BRAND_SAFETY_VIDEO_DISCOVERY = timedelta(minutes=30).total_seconds()
    RESEARCH_CACHING = timedelta(minutes=30).total_seconds()
    PRICING_TOOL_FILTERS_CACHING = timedelta(hours=3).total_seconds()
    GLOBAL_TRENDS_FILTERS_CACHING = timedelta(hours=3).total_seconds()
    FORECAST_TOOL_FILTERS_CACHING = timedelta(minutes=30).total_seconds()
    INDUSTRY_PERFORMANCE_CACHING = timedelta(minutes=30).total_seconds()
    PACING_REPORT_FILTERS = timedelta(minutes=2).total_seconds()
    INGEST_IAS = timedelta(hours=11).total_seconds()
