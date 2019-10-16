try:
    from .settings import *
except ImportError:
    pass

IS_TEST = True

try:
    from teamcity import is_running_under_teamcity

    if is_running_under_teamcity():
        TEST_RUNNER = "teamcity.django.TeamcityDjangoRunner"
except:
    pass
MIGRATION_MODULES = {
    "administration": None,
    "audit_tool": None,
    "auth": None,
    "ads_analyzer": None,
    "authtoken": None,
    "aw_creation": None,
    "aw_reporting": None,
    "brand_safety": None,
    "contenttypes": None,
    "django_celery_results": None,
    "email_reports": None,
    "keyword_tool": None,
    "segment": None,
    "sessions": None,
    "userprofile": None,
}

for logger_config in LOGGING["handlers"].values():
    logger_config["filters"] = ["hide_all"] \
                               + logger_config.get("filters", [])

AMAZON_S3_LOGO_STORAGE_URL_FORMAT = "https://s3.amazonaws.com/viewiq-test/logos/{}.png"

CELERY_BEAT_SCHEDULE = {}
CELERY_TASK_ALWAYS_EAGER = True
DMP_CELERY_TASK_ALWAYS_EAGER = True
APEX_HOST = "http://localhost:8000"
