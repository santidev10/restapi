try:
    from .settings import *
except ImportError:
    pass

IS_TEST = True

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

LOGGING["loggers"] = {
    "": {
        "handlers": [],
        "level": "CRITICAL"
    }
}
