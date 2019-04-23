SINGLE_DATABASE_API_URL = "http://10.0.2.205:10501/api/v1/"

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'HOST': '10.0.2.205', #PROD
        'NAME': 'saas-enterprise',
        'USER': 'admin_saas',
        'PASSWORD': 'kA1tWRRUyTLnNe2Hi8PL',
        'PORT': '',                      # Set to empty string for default.
    },
}

