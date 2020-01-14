"""
Django settings for saas project.

Generated by 'django-admin startproject' using Django 1.9.9.

For more information on this file, see
https://docs.djangoproject.com/en/1.9/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.9/ref/settings/
"""

import os
import socket
from datetime import date
import importlib

APM_ENABLED = os.getenv("APM_ENABLED", "0") == "1"

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.9/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '%ics*w%224v(ymhbgk4rpsqhs0ss7r(pxel%n(1fko6*5$-1=8'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS_ENV = os.getenv("ALLOWED_HOSTS")
if ALLOWED_HOSTS_ENV:
    ALLOWED_HOSTS = ALLOWED_HOSTS_ENV.split(",")
else:
    ALLOWED_HOSTS = []

# Application definition

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.staticfiles',
    'django.contrib.humanize'
)

PROJECT_APPS = (
    "administration",
    "ads_analyzer",
    "audit_tool",
    "aw_creation",
    "aw_reporting",
    "brand_safety",
    "cache",
    "channel",
    "email_reports",
    "healthcheck",
    "highlights",
    "keyword_tool",
    "related_tool",
    "segment",
    "userprofile",
)

THIRD_PARTY_APPS = (
    "rest_framework",
    "rest_framework.authtoken",
    "drf_yasg",
)
INSTALLED_APPS = INSTALLED_APPS + THIRD_PARTY_APPS + PROJECT_APPS

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'userprofile.middleware.ApexUserCheck',
]

ROOT_URLCONF = 'saas.urls.urls'

WSGI_APPLICATION = 'saas.wsgi.application'

STATIC_URL = '/static/'

STATIC_ROOT = os.path.join(BASE_DIR, 'static')
# STATICFILES_DIRS = (
#     os.path.join(BASE_DIR, 'static'),
# )

MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

MEDIA_URL = '/media/'

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [os.path.join(BASE_DIR, "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]
# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases

DATABASES = {
    'default': {
        # default values are for the TC only
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('DB_NAME', 'saas'),
        'USER': os.getenv('DB_USER', 'admin_saas'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'kA1tWRRUyTLnNe2Hi8PL'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', ''),  # Set to empty string for default.
    },
    'audit': {
        # default values are for the TC only
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('AUDIT_DB_NAME', 'audit'),
        'USER': os.getenv('AUDIT_DB_USER', 'admin_saas'),
        'PASSWORD': os.getenv('AUDIT_DB_PASSWORD', 'kA1tWRRUyTLnNe2Hi8PL'),
        'HOST': os.getenv('AUDIT_DB_HOST', 'localhost'),
        'PORT': os.getenv('AUDIT_DB_PORT', ''),  # Set to empty string for default.
    },

}
DATABASE_ROUTERS = ['saas.db_router.AuditDBRouter']

# Password validation
# https://docs.djangoproject.com/en/1.9/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/1.9/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

DEFAULT_TIMEZONE = 'America/Los_Angeles'

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.9/howto/static-files/

AUTH_USER_MODEL = "userprofile.UserProfile"
USER_DEFAULT_LOGO = "viewiq"
GOOGLE_APP_AUD = "832846444492-9j4sj19tkkrd3tpg7s8j5910l7kprg45.apps.googleusercontent.com"
GOOGLE_APP_SECRET = "<secret>"
GOOGLE_APP_OAUTH2_SCOPES = [
    "profile",
    "email",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtubepartner"]
GOOGLE_APP_OAUTH2_REDIRECT_URL = "postmessage"
GOOGLE_APP_OAUTH2_ORIGIN = "http://localhost"

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.TokenAuthentication',
    ),
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
    )
}

hostname = socket.gethostname()
try:
    ip = socket.gethostbyname(hostname)
except Exception as e:
    ip = socket.getfqdn(hostname)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "main_formatter",
        },
        "mail_admins": {
            "level": "ERROR",
            "filters": ["require_debug_false"],
            "class": "saas.admin_email_handler.LimitedAdminEmailHandler",
            "formatter": "detail_formatter",
        },
        "slack_aw_update": {
            "level": os.getenv("LOG_LEVEL_SLACK", "WARNING"),
            "class": "administration.notifications.SlackAWUpdateLoggingHandler",
            "filters": [
                "audience_not_found_warning_filter",
                "topic_not_found_warning_filter",
                "undefined_criteria_warning_filter",
                "require_debug_false",
            ],
        }
    },
    "loggers": {
        "aw_reporting.update": {
            "handlers": ["slack_aw_update"],
            "level": "INFO",
        },
        "": {
            "handlers": ["stdout", "mail_admins"],
            "level": os.getenv("DJANGO_LOG_LEVEL", "ERROR"),
        },
    },
    "formatters": {
        "main_formatter": {
            "format": "%(asctime)s %(levelname)-8s %(name)s:%(lineno)d > %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "detail_formatter": {
            "format": "HOST: {host}\nCWD: {cwd}\nIP: {ip}\n%(asctime)s "
                      "%(levelname)s %(filename)s line %(lineno)d: %(message)s"
                      "".format(host=hostname,
                                cwd=os.getcwd(),
                                ip=ip),
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "filters": {
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
        "hide_all": {
            "()": "django.utils.log.CallbackFilter",
            "callback": lambda r: 0,
        },
        "audience_not_found_warning_filter": {
            "()": "administration.notifications.AudienceNotFoundWarningLoggingFilter",
        },
        "topic_not_found_warning_filter": {
            "()": "administration.notifications.TopicNotFoundWarningLoggingFilter",
        },
        "undefined_criteria_warning_filter": {
            "()": "administration.notifications.UndefinedCriteriaWarningLoggingFilter",
        },
    }
}

SERVER_EMAIL = "viewiq-notifications@channelfactory.com"
SENDER_EMAIL_ADDRESS = SERVER_EMAIL
EMERGENCY_SENDER_EMAIL_ADDRESS = "emergency-viewiq@channelfactory.com"
EMAIL_BACKEND = "django_ses.SESBackend"
EXPORTS_EMAIL_ADDRESS = "export-notify@channelfactory.com"
ADMIN_EMAIL_LIMIT = 10000

PASSWORD_RESET_TIMEOUT_DAYS = 1

# this is default development key
YOUTUBE_API_DEVELOPER_KEY = 'AIzaSyDCDO_d-0vmFspHlEdf9eRaB_1bvMmJ2aI'
YOUTUBE_API_ALTERNATIVE_DEVELOPER_KEY = 'AIzaSyBYaLX2KAXsmXs3mbsTYBvjCe1-GCHoTX4'

from .configs.celery import *

CHANNEL_FACTORY_ACCOUNT_ID = "3386233102"
MIN_AW_FETCH_DATE = date(2012, 1, 1)

REGISTRATION_ACTION_EMAIL_ADDRESSES = [
    "maria.konareva@sigma.software",
    "sean.maguire@channelfactory.com",
]

CHANNEL_AUTHENTICATION_ACTION_EMAIL_ADDRESSES = [
    "maria.konareva@sigma.software",
    "sean.maguire@channelfactory.com",
]

CHANNEL_AUTHENTICATION_NOTIFY_TO = [
    "yuriy.matso@channelfactory.com",
    "aleksandr.yakovenko@sigma.software",
    "maria.konareva@sigma.software",
    "alexander.bykov@sigma.software",
    "sean.maguire@channelfactory.com",
    "andrii.dobrovolskyi@sigma.software"
]

CONTACT_FORM_EMAIL_ADDRESSES = [
    "maria.konareva@sigma.software",
    "sean.maguire@channelfactory.com",
]

AUDIT_TOOL_EMAIL_ADDRESSES = [
    "andrii.dobrovolskyi@sigma.software",
]

AUDIT_TOOL_EMAIL_RECIPIENTS = [
    "andrew.vonpelt@channelfactory.com",
    "bryan.ngo@channelfactory.com",
    "sean.maguire@channelfactory.com",
]

EMERGENCY_EMAIL_ADDRESSES = [
    "andrii.dobrovolskyi@sigma.software",
    "oleksandr.demianyshyn@sigma.software",
    "yaryna.kimak@sigma.software",
    "maria.konareva@sigma.software",
    "vira.horbushko@sigma.software",
    "kenneth.oh@channelfactory.com",
    "bryan.ngo@channelfactory.com",
    "george.su@channelfactory.com",
    "sean.maguire@channelfactory.com",
    "andrew.vonpelt@channelfactory.com"
]

ES_MONITORING_EMAIL_ADDRESSES = [
    "andrii.dobrovolskyi@sigma.software",
    "oleksandr.demianyshyn@sigma.software",
    "sergey.zhiltsov@sigma.software",
    "yaryna.kimak@sigma.software",
    "andrew.vonpelt@channelfactory.com",
    "kenneth.oh@channelfactory.com",
    "bryan.ngo@channelfactory.com",
    "george.su@channelfactory.com",
    "sean.maguire@channelfactory.com"
]

UI_ERROR_REPORT_EMAIL_ADDRESSES = [
    "sean.maguire@channelfactory.com",
    "servando.berna@channelfactory.com",
]

SALESFORCE_UPDATES_ADDRESSES = []
SALESFORCE_UPDATE_DELAY_DAYS = 5

DEBUG_EMAIL_NOTIFICATIONS = True

MS_CHANNELFACTORY_EMAIL = "ms@channelfactory.com"

TESTIMONIALS = {
    "UCpT9kL2Eba91BB9CK6wJ4Pg": "HKq3esKhu14",
    "UCZG-C5esGZyVfxO2qXa1Zmw": "IBEvDNaWGYY",
}

IS_TEST = False

CACHE_ENABLED = False
CACHE_MAIN_KEY = 'http_cache_requests_history'
CACHE_KEY_PREFIX = 'http_cache_path_'
CACHE_TIMEOUT = 1800
CACHE_HISTORY_LIMIT = 5000
CACHE_PAGES_LIMIT = 500
CACHE_BASE_URL = 'http://localhost:8000'
CACHE_AUTH_TOKEN = 'put_auth_token_here'

HOST = "https://viewiq.channelfactory.com"
APEX_HOST = "https://apex.viewiq.com"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)

CF_AD_OPS_DIRECTORS = [
    "george.ritter@channelfactory.com",
]

CUSTOM_AUTH_FLAGS = {
    # "user@example.com": {
    #    "hide_something": True,
    #    "show_something_else": True,
    #    "logo_url": "https://s3.amazonaws.com/viewiq-prod/logos/super_user.png",
    # },
}

AMAZON_S3_BUCKET_NAME = "viewiq-dev"
AMAZON_S3_REPORTS_BUCKET_NAME = "viewiq-reports-local"
AMAZON_S3_AUDITS_FILES_BUCKET_NAME = "viewiq-audit-files"
AMAZON_S3_AUDITS_EXPORTS_BUCKET_NAME = "viewiq-audit-exports"
AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME = "viewiq-dev-custom-segments"
AMAZON_S3_ACCESS_KEY_ID = None
AMAZON_S3_SECRET_ACCESS_KEY = None
AMAZON_S3_LOGO_STORAGE_URL_FORMAT = "https://s3.amazonaws.com/viewiq-prod/logos/{}.png"

MAX_AVATAR_SIZE_MB = 10.

DASHBOARD_PERFORMANCE_REPORT_LIMIT = 1048575  # excel row limit minus one row for footer
AUTOPILOT_API_KEY = "dd069a2d588d4dce95fe134b553ca5df"

AW_UPDATE_SLACK_WEBHOOK_NAME = "aw_update"

SLACK_WEBHOOKS = {
    AW_UPDATE_SLACK_WEBHOOK_NAME: "https://hooks.slack.com/services/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",
}

SWAGGER_SETTINGS = {
    'SECURITY_DEFINITIONS': {
        'Bearer': {
            'type': 'apiKey',
            'name': 'Authorization',
            'in': 'header'
        }
    },
    'LOGIN_URL': "/docs/login/",
    'LOGOUT_URL': "/docs/logout/",
}

TEMPDIR = "/tmp"

MAX_SEGMENT_TO_AGGREGATE = 10000

USE_LEGACY_BRAND_SAFETY = True

ES_CACHE_ENABLED = False

from es_components.config import *

BRAND_SAFETY_CHANNEL_INDEX = ""
BRAND_SAFETY_VIDEO_INDEX = ""
BRAND_SAFETY_TYPE = ""
ELASTIC_SEARCH_REQUEST_TIMEOUT = 600

REPORT_EXPIRATION_PERIOD = 24
REPORT_VISIBLE_PERIOD = 90  # in days
SHOW_CAMPAIGNS_FOR_LAST_YEARS = 1

if APM_ENABLED:
    ELASTIC_APM = {
        "SERVICE_NAME": "viewiq-api",
        # Use if APM Server requires a token
        "SECRET_TOKEN": "",
        "SERVER_URL": "http://apm-server:8200",
        "DEBUG": True,
    }
    MIDDLEWARE = ['elasticapm.contrib.django.middleware.TracingMiddleware'] + MIDDLEWARE
    INSTALLED_APPS = INSTALLED_APPS + ('elasticapm.contrib.django',)

try:
    from .local_settings import *
except ImportError:
    pass
