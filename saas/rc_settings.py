DATABASES = {
    'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'saas-enterprise',
            'USER': 'admin_saas',
            'PASSWORD': 'kA1tWRRUyTLnNe2Hi8PL',
            'HOST': 'pg-dev-rc.cumlpskicipr.us-east-1.rds.amazonaws.com',
            'PORT': '',
    },
}
SINGLE_DATABASE_API_URL = "http://10.0.2.205:10501/api/v1/"
