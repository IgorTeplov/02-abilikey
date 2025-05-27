#!/bin/bash

/usr/sbin/rsyslogd -n -iNONE &
service cron start
crontab /root/app/cronfile
service cron reload
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
