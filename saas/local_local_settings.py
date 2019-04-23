################### LOCAL  ###################
#
DATABASES = {
    #
    ## LOCAL ######
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test',
        'USER': 'ken',
        'PASSWORD': 'ken',
        'HOST': '127.0.0.1',
        'PORT': '',
    },
}
# PROD SINGLEDB
SINGLE_DATABASE_API_URL = "http://10.0.2.205:10501/api/v1/"
