import os

try:
    from .settings import *
except ImportError:
    pass


DEBUG = True
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'main_formatter',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
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

SINGLE_DATABASE_API_URL = "http://sdb:10500/api/v1/"

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': '',
        'HOST': 'saas-postgres',
        'PORT': '',  # Set to empty string for default.
    }
}
