"""
Django settings for saas project.

Generated by 'django-admin startproject' using Django 1.9.9.

For more information on this file, see
https://docs.djangoproject.com/en/1.9/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.9/ref/settings/
"""

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.9/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '%ics*w%224v(ymhbgk4rpsqhs0ss7r(pxel%n(1fko6*5$-1=8'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

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
    "payments",
    "channel"
)

THIRD_PARTY_APPS = (
    "rest_framework",
    "rest_framework.authtoken",
    "djcelery",
)

INSTALLED_APPS = INSTALLED_APPS + THIRD_PARTY_APPS + PROJECT_APPS

MIDDLEWARE_CLASSES = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
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

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.9/howto/static-files/

AUTH_USER_MODEL = "userprofile.UserProfile"
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

LOGS_DIRECTORY = '.'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'main_formatter',
        },
        'file': {
            'level': 'ERROR',
            'filename': os.path.join(LOGS_DIRECTORY, 'iq_errors.log'),
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'main_formatter',
        },
        'mail_developers': {
            'level': 'CRITICAL',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler',
            'formatter': 'detail_formatter',
        }
    },
    'loggers': {
        'segment_creating': {
            'handlers': ['console', 'file'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
            'propagate': False
        },
        '': {
            'handlers': ['console', 'file', 'mail_developers'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
        },
    },
    'formatters': {
        'main_formatter': {
            'format': '%(asctime)s %(levelname)s: %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S",
        },
        'detail_formatter': {
            'format': '%(asctime)s %(levelname)s %(filename)s '
                      'line %(lineno)d: %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S",
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.CallbackFilter',
            'callback': lambda r: not DEBUG,
        }
    }
}

SENDER_EMAIL_ADDRESS = "chf-no-reply@channelfactory.com"
EMAIL_HOST = "localhost"
EMAIL_PORT = 1025
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

PASSWORD_RESET_TIMEOUT_DAYS = 1

# this is default development key
YOUTUBE_API_DEVELOPER_KEY = 'AIzaSyDCDO_d-0vmFspHlEdf9eRaB_1bvMmJ2aI'

# stripe user keys
STRIPE_PUBLIC_KEY = None
STRIPE_SECRET_KEY = None

SINGLE_DATABASE_API_URL = "http://10.0.2.39:10500/api/v1/"
IQ_API_URL = "https://iq.channelfactory.com/api/v1/"

import djcelery

djcelery.setup_loader()
CELERY_TASK_RESULT_EXPIRES = 18000
CELERYD_TASK_ERROR_EMAILS = False
CELERY_RESULT_BACKEND = "redis://"
CELERY_REDIS_HOST = "localhost"
CELERY_REDIS_PORT = 6379
CELERY_REDIS_DB = 0
CELERY_ACKS_LATE = True
CELERYD_PREFETCH_MULTIPLIER = 1

BROKER_URL = "redis://localhost:6379/0"

KW_TOOL_KEY = "Qi3mxPnm"

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
#    "yuriy.matso@channelfactory.com",
    "anna.chumak@sigma.software",
    "maria.konareva@sigma.software",
    "yulia.prokudina@sigma.software",
]

CHANNEL_AUTHENTICATION_ACTION_EMAIL_ADDRESSES = [
    "anna.chumak@sigma.software",
    "maria.konareva@sigma.software",
    "yulia.prokudina@sigma.software",
]

PAYMENT_ACTION_EMAIL_ADDRESSES = [
    "alexander.dobrzhansky@sigma.software",
    "anna.chumak@sigma.software",
    "maria.konareva@sigma.software",
    "yulia.prokudina@sigma.software",
]

CONTACT_FORM_EMAIL_ADDRESSES = [
    "anna.chumak@sigma.software",
    "maria.konareva@sigma.software",
    "yulia.prokudina@sigma.software",
]

MS_CHANNELFACTORY_EMAIL = "ms@channelfactory.com"

DEFAULT_USER_ACCESS = [
    {'name': 'Highlights', 'value': True, },
    {'name': 'Research', 'value': False, },
    {'name': 'Segments', 'value': False, },
    {'name': 'Segments - pre-baked segments', 'value': False, },
    {'name': 'Media buying', 'value': False, },
    {'name': 'Auth channels and audience data', 'value': False, },
]

ACCESS_PLANS = {
    'free': {
        'hidden': False,
        'access': DEFAULT_USER_ACCESS,
    },
    'professional': {
        'hidden': False,
        'access': [
            {'name': 'Highlights', 'value': True, },
            {'name': 'Research', 'value': True, },
            {'name': 'Segments', 'value': True, },
            {'name': 'Segments - pre-baked segments', 'value': False, },
            {'name': 'Media buying', 'value': False, },
            {'name': 'Auth channels and audience data', 'value': False, },
        ]
    },
    'enterprise': {
        'hidden': True,
        'access': [
            {'name': 'Highlights', 'value': True, },
            {'name': 'Research', 'value': True, },
            {'name': 'Segments', 'value': True, },
            {'name': 'Segments - pre-baked segments', 'value': True, },
            {'name': 'Media buying', 'value': True, },
            {'name': 'Auth channels and audience data', 'value': True, },
        ]
    },
}

DEFAULT_USER_PERMISSIONS = {
    'channel': {'list': False, 'filter': False, 'audience': False, 'aw_performance': False,'details': False},
    'video': {'list': False, 'filter': False, 'audience': False, 'aw_performance': False, 'details': False},
    'keyword': {'list': False, 'details': False, },
    'segment': {
        'channel': {'all': False, 'private': False},
        'video': {'all': False, 'private': False},
        'keyword': {'all': False, 'private': False},
    },
    'view': {
        'create_and_manage_campaigns': False,
        'performance': False,
        'trends': False,
        'benchmarks': False,
        'highlights': True,
        'pre_baked_segments': False,
        'media_buying': False,
    },
    'settings': {
        'my_yt_channels': True,
        'my_aw_accounts': False,
    },
}

USER_ACCESS_LOGIC = {
    'Highlights': {
        'view': {'highlights': True, },
    },
    'Research': {
        'channel': {'list': True, 'filter': True, 'details': True, },
        'video': {'list': True, 'filter': True, 'details': True, },
        'keyword': {'list': True, 'filter': True, },
    },
    'Segments': {
        'segment': {
            'channel': {'private': True, },
            'video': {'private': True, },
            'keyword': {'private': True, },
        },
    },
    'Segments - pre-baked segments': {
        'actions': [{'input': False, 'action_type': 'post', 'access': ['Research'], }, ],
        'view': {'pre_baked_segments': True, },
        'channel': {'list': True, },
        'video': {'list': True, },
        'keyword': {'list': True, },
    },
    'Media buying': {
        'view': {'media_buying': True, },
        'settings': {'my_aw_accounts': True, },
    },
    'Auth channels and audience data': {
        'channel': {'audience': True, 'aw_performance': True, },
        'video': {'audience': True, 'aw_performance': True, },
    },
}

DEFAULT_ACCESS_PLAN_NAME = 'free'
CHANNEL_AUTHENTICATION_PLAN_NAME = 'professional'

VENDOR = 'viewiq'

TESTIMONIALS = {
    "UCpT9kL2Eba91BB9CK6wJ4Pg": "HKq3esKhu14",
    "UCZG-C5esGZyVfxO2qXa1Zmw": "IBEvDNaWGYY",
}

try:
    from .local_settings import *
except ImportError:
    pass
