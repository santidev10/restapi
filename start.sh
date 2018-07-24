#!/usr/bin/env bash

/wait-for-it.sh -t 0 pg:5432
./manage.py migrate
./manage.py shell -c "from django.contrib.auth import get_user_model; not get_user_model().objects.filter(email='admin@admin.admin').exists() and get_user_model().objects.create_superuser('admin', 'admin@admin.admin', 'admin')"
/wait-for-it.sh -t 0 sdb:10500
./manage.py runserver 0.0.0.0:5000