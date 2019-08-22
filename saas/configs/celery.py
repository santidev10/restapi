import os
import re

from celery.schedules import crontab

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_API_PORT = os.getenv("RABBITMQ_API_PORT", 15672)
RABBITMQ_AMQP_PORT = os.getenv("RABBITMQ_AMQP_PORT", 5672)

RABBITMQ_API_USER = os.getenv("RABBITMQ_API_USER", "guest")
RABBITMQ_API_PASSWORD = os.getenv("RABBITMQ_API_PASSWORD", "guest")

RABBITMQ_API_URL = "{host}:{port}".format(host=RABBITMQ_HOST, port=RABBITMQ_API_PORT)
CELERY_BROKER_URL = "amqp://{host}:{port}/restapi".format(host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)

DMP_CELERY_BROKER_URL = "amqp://{host}:{port}/dmp".format(host=RABBITMQ_HOST, port=RABBITMQ_AMQP_PORT)
DMP_CELERY_RESULT_BACKEND = os.getenv("DMP_RESULT_BACKEND", "elasticsearch://example.com:9200/celery/task_result")

CELERY_TIMEZONE = "UTC"
CELERY_BEAT_SCHEDULE = {
    "full-aw-update": {
        "task": "aw_reporting.update.update_aw_accounts.update_aw_accounts",
        "schedule": crontab(hour="5,13,21", minute="0"),  # each 8 hours including 6AM in LA
    },
    "hourly-aw_update": {
        "task": "aw_reporting.update.update_aw_accounts_hourly_stats.update_aw_accounts_hourly_stats",
        "schedule": crontab(hour="*", minute="0"),
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
            reports=["CampaignUnderMargin", "TechFeeCapExceeded", "CampaignUnderPacing", "CampaignOverPacing"],
            debug=True,
        ),
    },
    "weekday-campaign-reports": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30", day_of_week="Mon,Tue,Wed,Thu,Fri"),
        "kwargs": dict(
            reports=["DailyCampaignReport"],
            roles="Account Manager",
            debug=True,
        ),
    },
    "weekend-campaign-reports": {
        "task": "email_reports.tasks.send_daily_email_reports",
        "schedule": crontab(hour="13", minute="30", day_of_week="Sun,Sat"),
        "kwargs": dict(
            reports=["DailyCampaignReport"],
            roles="Account Manager,Ad Ops Manager",
            debug=True,
        ),
    },
    "update-audiences": {
        "task": "aw_reporting.update.tasks.update_audiences.update_audiences_from_aw",
        "schedule": crontab(day_of_month="1", hour="0", minute="0"),
    },
    "recreate-demo-data": {
        "task": "aw_reporting.demo.recreate_demo_data.recreate_demo_data",
        "schedule": crontab(hour="0", minute="0"),
    },
    "update-videos-percentiles": {
        "task": "video.tasks.update_videos_percentiles",
        "schedule": 3600,
    },
    "update-channels-percentiles": {
        "task": "channel.tasks.update_channels_percentiles",
        "schedule": 3600,
    },
    "update-keywords-percentiles": {
        "task": "keywords.tasks.update_keywords_percentiles",
        "schedule": 3600,
    },
}
CELERY_RESULT_BACKEND = "django-db"

# Suggestion from this thread https://github.com/celery/celery/issues/4226
CELERY_BROKER_POOL_LIMIT = None


class Queue:
    DEFAULT = "celery"
    REPORTS = "reports"
    DELIVERY_STATISTIC_UPDATE = "delivery_statistic"
    EMAIL_REPORTS = "email_reports"


CELERY_ROUTES_PREPARED = [
    ("aw_reporting.update.*", {"queue": Queue.DELIVERY_STATISTIC_UPDATE}),
    ("aw_reporting.reports.*", {"queue": Queue.REPORTS}),
    ("email_reports.*", {"queue": Queue.EMAIL_REPORTS}),
    ("*", {"queue": Queue.DEFAULT}),
]
# dirty fix for celery. fixes AttributeError
re._pattern_type = re.Pattern

CELERY_TASK_ROUTES = (CELERY_ROUTES_PREPARED,)
