#!/usr/bin/env bash

./init.sh
/wait-for-it.sh -t 0 pg:5432
/wait-for-it.sh -t 0 sdb:10500
./manage.py runserver 0.0.0.0:5000