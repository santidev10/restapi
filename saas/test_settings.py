try:
    from .settings import *
except ImportError:
    pass

IS_TEST = True
DISABLE_ACCOUNT_CREATION_AUTO_CREATING = True

MIGRATION_MODULES = {
    "administration": None,
    "auth": None,
    "authtoken": None,
    "aw_creation": None,
    "aw_reporting": None,
    "contenttypes": None,
    "djcelery": None,
    "keyword_tool": None,
    "landing": None,
    "payments": None,
    "segment": None,
    "sessions": None,
    "userprofile": None,
    "email_reports": None,
}
for logger_config in LOGGING["handlers"].values():
    logger_config["filters"] = ["hide_all"] \
                               + logger_config.get("filters", [])

AMAZON_S3_LOGO_STORAGE_URL_FORMAT = "https://s3.amazonaws.com/viewiq-rc/logos/{}.png"
