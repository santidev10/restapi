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

from teamcity import is_running_under_teamcity, teamcity_presence_env_var

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
)

PROJECT_APPS = (
    "aw_creation",
    "aw_reporting",
    "userprofile",
    "segment",
    "keyword_tool",
    "landing",
    "administration",
    "channel",
    "email_reports",
    "audit_tool",
)

THIRD_PARTY_APPS = (
    "django_celery_results",
    "rest_framework",
    "rest_framework.authtoken",
)

INSTALLED_APPS = INSTALLED_APPS + THIRD_PARTY_APPS + PROJECT_APPS

MIDDLEWARE_CLASSES = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'utils.index_middleware.IndexMiddleware',
]

ROOT_URLCONF = 'saas.urls'

WSGI_APPLICATION = 'saas.wsgi.application'

STATIC_URL = '/static/'

STATICFILES_DIRS = (
    os.path.join(BASE_DIR, 'static'),
)

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
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'saas',
        'USER': 'admin_saas',
        'PASSWORD': 'kA1tWRRUyTLnNe2Hi8PL',
        'HOST': 'localhost',
        'PORT': '',  # Set to empty string for default.
    }
}

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

LOGS_DIRECTORY = 'logs'

DJANGO_LOG_FILE = os.getenv("DJANGO_LOG_FILE", "viewiq.log")
hostname = socket.gethostname()
ip = socket.gethostbyname(hostname)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'main_formatter',
            'filters': ['require_debug_true']
        },
        'file': {
            'filename': os.path.join(LOGS_DIRECTORY, DJANGO_LOG_FILE),
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'main_formatter',
        },
        'file_googleads': {
            'filename': os.path.join(LOGS_DIRECTORY, "googleads.log"),
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'main_formatter',
        },
        'file_updates': {
            'filename': os.path.join(LOGS_DIRECTORY, "aw_update.log"),
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'main_formatter',
        },
        'file_celery': {
            'filename': os.path.join(LOGS_DIRECTORY, "celery_info.log"),
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'main_formatter',
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler',
            'formatter': 'detail_formatter',
        },
        "slack_aw_update": {
            "level": "INFO",
            "class": "administration.notifications.SlackAWUpdateLoggingHandler",
            "filters": [
                "audience_not_found_warning_filter",
                "topic_not_found_warning_filter",
                "undefined_criteria_warning_filter",
                "require_debug_false",
            ],
        }
    },
    'loggers': {
        "googleads": {
            "handlers": ["file_googleads"],
            "level": "WARNING",
        },
        "aw_reporting.update": {
            "handlers": ["file_updates", "slack_aw_update", "mail_admins"],
            "level": "DEBUG",
        },
        "celery": {
            "handlers": ["file_celery"],
            "level": "INFO",
        },
        '': {
            'handlers': ['console', 'file', "mail_admins"],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'ERROR'),
        },
    },
    'formatters': {
        'main_formatter': {
            'format': '%(asctime)s %(levelname)s: %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S",
        },
        'detail_formatter': {
            'format': 'HOST: {host}\nCWD: {cwd}\nIP: {ip}\n%(asctime)s '
                      '%(levelname)s %(filename)s line %(lineno)d: %(message)s'
                      ''.format(host=hostname,
                                cwd=os.getcwd(),
                                ip=ip),
            'datefmt': "%Y-%m-%d %H:%M:%S",
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
        "hide_all": {
            '()': 'django.utils.log.CallbackFilter',
            'callback': lambda r: 0,
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

SENDER_EMAIL_ADDRESS = "chf-no-reply@channelfactory.com"
EMAIL_HOST = "localhost"
EMAIL_PORT = os.getenv("EMAIL_PORT", None) or 1025
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

PASSWORD_RESET_TIMEOUT_DAYS = 1

# this is default development key
YOUTUBE_API_DEVELOPER_KEY = 'AIzaSyDCDO_d-0vmFspHlEdf9eRaB_1bvMmJ2aI'

SINGLE_DATABASE_API_HOST = os.getenv("SINGLE_DATABASE_API_HOST", "10.0.2.39")
SINGLE_DATABASE_API_URL = "http://{host}:10500/api/v1/".format(host=SINGLE_DATABASE_API_HOST)

from .configs.celery import *

CHANNEL_FACTORY_ACCOUNT_ID = "3386233102"
MIN_AW_FETCH_DATE = date(2012, 1, 1)

# landing page settings
LANDING_SUBJECT = [
    "General",
    "Sales",
    "Technical Support"
]

LANDING_CONTACTS = {
    'default': [
        'chf.team@sigma.software',
    ],
}

REGISTRATION_ACTION_EMAIL_ADDRESSES = [
    "alex.klinovoy@sigma.software",
    "maria.konareva@sigma.software",
    "maryna.antonova@sigma.software",
    "yulia.prokudina@sigma.software",
]

CHANNEL_AUTHENTICATION_ACTION_EMAIL_ADDRESSES = [
    "alex.klinovoy@sigma.software",
    "maria.konareva@sigma.software",
    "maryna.antonova@sigma.software",
    "yulia.prokudina@sigma.software",
]

PAYMENT_ACTION_EMAIL_ADDRESSES = [
    "alexander.dobrzhansky@sigma.software",
    "anna.chumak@sigma.software",
    "maria.konareva@sigma.software",
    "yulia.prokudina@sigma.software",
]

CONTACT_FORM_EMAIL_ADDRESSES = [
    "alex.klinovoy@sigma.software",
    "maria.konareva@sigma.software",
    "maryna.antonova@sigma.software",
    "yulia.prokudina@sigma.software",
]

AUDIT_TOOL_EMAIL_ADDRESSES = [
    "andrii.dobrovolskyi@sigma.software",
]

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

CF_AD_OPS_DIRECTORS = [
    ('Kim, John', "john.kim@channelfactory.com"),
]

CUSTOM_AUTH_FLAGS = {
    # "user@example.com": {
    #    "hide_something": True,
    #    "show_something_else": True,
    #    "logo_url": "https://s3.amazonaws.com/viewiq-prod/logos/super_user.png",
    # },
}


# patch checking if TC. Hopefully it will be included into teamcity-messages > 1.21
def is_running_under_teamcity():
    return bool(os.getenv(teamcity_presence_env_var))


if is_running_under_teamcity():
    TEST_RUNNER = "teamcity.django.TeamcityDjangoRunner"

AMAZON_S3_BUCKET_NAME = "viewiq-dev"
AMAZON_S3_ACCESS_KEY_ID = "<put_aws_access_key_id_here>"
AMAZON_S3_SECRET_ACCESS_KEY = "<put_aws_secret_access_key>"
AMAZON_S3_LOGO_STORAGE_URL_FORMAT = "https://s3.amazonaws.com/viewiq-prod/logos/{}.png"

MAX_AVATAR_SIZE_MB = 10.

DASHBOARD_PERFORMANCE_REPORT_LIMIT = 1048575  # excel row limit minus one row for footer
AUTOPILOT_API_KEY = "dd069a2d588d4dce95fe134b553ca5df"

AW_UPDATE_SLACK_WEBHOOK_NAME = "aw_update"

SLACK_WEBHOOKS = {
    AW_UPDATE_SLACK_WEBHOOK_NAME: "https://hooks.slack.com/services/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",
}

try:
    from .local_settings import *
except ImportError:
    pass
