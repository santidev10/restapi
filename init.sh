#!/usr/bin/env bash

DB_HOST=$(./manage.py shell -c "from django.conf import settings; print(settings.DATABASES['default']['HOST'])")

if [[ ${DB_HOST} != 'pg' ]] ;
then
    echo "WARNING: non-docker database detected. Skipping init.sh script"
    exit 0
fi

./manage.py migrate
./manage.py shell -c "from django.contrib.auth import get_user_model; not get_user_model().objects.filter(email='admin@admin.admin').exists() and get_user_model().objects.create_superuser('admin', 'admin@admin.admin', 'admin')"
