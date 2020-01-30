import os
import re
from datetime import timedelta

from celery.schedules import crontab

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_API_PORT = os.getenv("RABBITMQ_API_PORT", 15672)
RABBITMQ_AMQP_PORT = os.getenv("RABBITMQ_AMQP_PORT", 5672)

RABBITMQ_API_USER = os.getenv("RABBITMQ_API_USER", "guest")
RABBITMQ_API_PASSWORD = os.getenv("RABBITMQ_API_PASSWORD", "guest")

RABBITMQ_API_URL = "{host}:{port}".format(host=RABBITMQ_HOST, port=RABBITMQ_API_PORT)
CELERY_BROKER_URL = "amqp://{host}:{port}/restapi".format(host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "elasticsearch://example.com:9200/celery/task_result")
CELERY_RESULT_EXTENDED = True

DMP_CELERY_BROKER_URL = "amqp://{host}:{port}/dmp".format(host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)
DMP_CELERY_RESULT_BACKEND = os.getenv("DMP_RESULT_BACKEND", CELERY_RESULT_BACKEND)
DMP_CELERY_RESULT_EXTENDED = True

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
    "full-sf-update": {
        "task": "aw_reporting.update.update_salesforce_data.update_salesforce_data",
        "schedule": crontab(hour="*", minute="0"),
        "kwargs": dict(do_update=os.getenv("DO_SALESFORCE_UPDATE", "0") == "1")
    },
    "daily_email_notifications": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30"),
        "kwargs": dict(
            reports=["CampaignUnderMargin", "TechFeeCapExceeded", "CampaignUnderPacing",
                     "CampaignOverPacing", "ESMonitoringEmailReport"],
        ),
    },
    "weekday-campaign-reports": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30", day_of_week="Mon,Tue,Wed,Thu,Fri"),
        "kwargs": dict(
            reports=["DailyCampaignReport"],
            roles="Account Manager",
        ),
    },
    "weekend-campaign-reports": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30", day_of_week="Sun,Sat"),
        "kwargs": dict(
            reports=["DailyCampaignReport"],
            roles="Account Manager,Ad Ops Manager",
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
        "task": "audit_tool.tasks.pull_custom_transcripts.pull_custom_transcripts",
        "schedule": 60
    },
    "cache-video-aggregations": {
        "task": "cache.tasks.cache_video_aggregations.cache_video_aggregations",
        "schedule": crontab(hour="*"),
    },
    "cache-channel-aggregations": {
        "task": "cache.tasks.cache_channel_aggregations.cache_channel_aggregations",
        "schedule": crontab(hour="*"),
    },
    "cache-keyword-aggregations": {
        "task": "cache.tasks.cache_keyword_aggregations.cache_keyword_aggregations",
        "schedule": crontab(hour="*"),
    },
    "cache-research-videos-defaults": {
        "task": "cache.tasks.cache_research_videos_defaults.cache_research_videos_defaults",
        "schedule": crontab(hour="*"),
    },
    "cache-research-channels-defaults": {
        "task": "cache.tasks.cache_research_channels_defaults.cache_research_channels_defaults",
        "schedule": crontab(hour="*"),
    },
    "cache-research-keywords-defaults": {
        "task": "cache.tasks.cache_research_keywords_defaults.cache_research_keywords_defaults",
        "schedule": crontab(hour="*"),
    },
    "generate_persistent_segments": {
        "task": "segment.tasks.generate_persistent_segments.generate_persistent_segments",
        "schedule": crontab(hour="*"),
    },
    "brand_safety_channel_discovery": {
        "task": "brand_safety.tasks.channel_discovery.channel_discovery_scheduler",
        "schedule": 60 * 5,
    },
    "brand_safety_channel_outdated": {
        "task": "brand_safety.tasks.channel_outdated.channel_outdated_scheduler",
        "schedule": 60 * 5,
    },
    "brand_safety_video_discovery": {
        "task": "brand_safety.tasks.video_discovery.video_discovery_scheduler",
        "schedule": 60 * 5,
    },
    "audit_tool_refresh_vetting_items": {

    }
}


# Suggestion from this thread https://github.com/celery/celery/issues/4226
CELERY_BROKER_POOL_LIMIT = None


class Queue:
    DEFAULT = "celery"
    REPORTS = "reports"
    EXPORT = "export"
    SEGMENTS = "segments"
    DELIVERY_STATISTIC_UPDATE = "delivery_statistic"
    EMAIL_REPORTS = "email_reports"
    HOURLY_STATISTIC = "hourly_statistic"
    CUSTOM_TRANSCRIPTS = "custom_transcripts"
    CACHE_RESEARCH = "cache_research"
    BRAND_SAFETY_CHANNEL_LIGHT = "brand_safety_channel_light"
    BRAND_SAFETY_CHANNEL_PRIORITY = "brand_safety_channel_priority"
    BRAND_SAFETY_VIDEO_PRIORITY = "brand_safety_video_priority"
    SCHEDULERS = "schedulers"


CELERY_ROUTES_PREPARED = [
    ("audit_tool.tasks.pull_custom_transcripts.*", {"queue": Queue.CUSTOM_TRANSCRIPTS}),
    ("aw_reporting.google_ads.tasks.update_campaigns.*", {"queue": Queue.HOURLY_STATISTIC}),
    ("aw_reporting.google_ads.tasks.update_without_campaigns.*", {"queue": Queue.DELIVERY_STATISTIC_UPDATE}),
    ("aw_reporting.update.*", {"queue": Queue.HOURLY_STATISTIC}),
    ("aw_reporting.reports.*", {"queue": Queue.REPORTS}),
    ("cache.tasks.*", {"queue": Queue.CACHE_RESEARCH}),
    ("email_reports.*", {"queue": Queue.EMAIL_REPORTS}),
    ("*export*", {"queue": Queue.EXPORT}),
    ("segment.tasks.*", {"queue": Queue.SEGMENTS}),
    ("*_scheduler", {"queue": Queue.SCHEDULERS}),
    ("*", {"queue": Queue.DEFAULT}),
]
# dirty fix for celery. fixes AttributeError
re._pattern_type = re.Pattern

CELERY_TASK_ROUTES = (CELERY_ROUTES_PREPARED,)


class TaskExpiration:
    FULL_AW_UPDATE = timedelta(hours=8).total_seconds()
    FULL_AW_ACCOUNT_UPDATE = timedelta(hours=1).total_seconds()
    HOURLY_AW_UPDATE = timedelta(hours=1).total_seconds()
    FULL_SF_UPDATE = timedelta(hours=1).total_seconds()
    CUSTOM_TRANSCRIPTS = timedelta(minutes=30).total_seconds()
    BRAND_SAFETY_CHANNEL_DISCOVERY = timedelta(minutes=30).total_seconds()
    BRAND_SAFETY_CHANNEL_OUTDATED = timedelta(hours=2).total_seconds()
    BRAND_SAFETY_VIDEO_DISCOVERY = timedelta(minutes=30).total_seconds()
    RESEARCH_CACHING = timedelta(minutes=30).total_seconds()


class TaskTimeout:
    FULL_AW_UPDATE = timedelta(hours=8).total_seconds()
    FULL_AW_ACCOUNT_UPDATE = timedelta(hours=1).total_seconds()
    HOURLY_AW_UPDATE = timedelta(hours=1).total_seconds()
    FULL_SF_UPDATE = timedelta(hours=1).total_seconds()
    CUSTOM_TRANSCRIPTS = timedelta(minutes=30).total_seconds()
    BRAND_SAFETY_CHANNEL_DISCOVERY = timedelta(minutes=30).total_seconds()
    BRAND_SAFETY_CHANNEL_OUTDATED = timedelta(hours=2).total_seconds()
    BRAND_SAFETY_VIDEO_DISCOVERY = timedelta(minutes=30).total_seconds()
    RESEARCH_CACHING = timedelta(minutes=30).total_seconds()
