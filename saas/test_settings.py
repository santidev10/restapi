try:
    from .settings import *
except ImportError:
    pass

IS_TEST = True

try:
    from teamcity import is_running_under_teamcity

    if is_running_under_teamcity():
        TEST_RUNNER = "teamcity.django.TeamcityDjangoRunner"
except BaseException:
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
    "cache": None,
    "contenttypes": None,
    "email_reports": None,
    "keyword_tool": None,
    "oauth": None,
    "performiq": None,
    "segment": None,
    "sessions": None,
    "transcripts": None,
    "userprofile": None,
}

FILE_UPLOAD_HANDLERS = (
    "django.core.files.uploadhandler.MemoryFileUploadHandler",
    "django.core.files.uploadhandler.TemporaryFileUploadHandler"
)

for logger_config in LOGGING["handlers"].values():
    logger_config["filters"] = ["hide_all"] \
                               + logger_config.get("filters", [])

AMAZON_S3_LOGO_STORAGE_URL_FORMAT = "https://s3.amazonaws.com/viewiq-test/logos/{}.png"

CELERY_BEAT_SCHEDULE = {}
CELERY_TASK_ALWAYS_EAGER = True
DMP_CELERY_TASK_ALWAYS_EAGER = True
APEX_HOST = "http://localhost:8000"
DOMAIN_MANAGEMENT_PERMISSIONS = ()
