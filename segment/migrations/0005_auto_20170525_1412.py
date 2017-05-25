# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-05-25 14:12
from __future__ import unicode_literals

import datetime
from django.db import migrations, models
from django.utils.timezone import utc


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0004_auto_20170517_1350'),
    ]

    operations = [
        migrations.AddField(
            model_name='segment',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=datetime.datetime(2017, 5, 25, 14, 12, 19, 413290, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='segment',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, default=datetime.datetime(2017, 5, 25, 14, 12, 24, 125225, tzinfo=utc)),
            preserve_default=False,
        ),
    ]
