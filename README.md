# SaaS
## Channel Factory


## Docker
### Start full application
> Prestep: create docker images for UI. See readme in related repo.
```shell
docker-compose up --build
```
Wait for "Compilation success" message from web service and checkout the VIQ on the `http://localhost:8000` 

### Run tests
```shell
docker-compose -f ./docker-compose.dev.yml run --rm api ./manage.py test --no-input --settings=saas.test_settings
```
> for TC don't forget to rebuild image beforehand
```shell
docker-compose build
```

### local_settings.py example
```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'saas',
        'USER': 'postgres',
        'PASSWORD': 'password',
        'HOST': 'pg',
        'PORT': '',  # Set to empty string for default.
    }
}
```

### Init
If you are starting some particular service then docker infrastructure may be not completely setup (i.e database is not prepared).
To solve this run `init.sh` script in the `api` container
