# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-12-20 15:56
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0010_auto_20171106_1101'),
    ]

    operations = [
        migrations.AddField(
            model_name='segmentkeyword',
            name='keywords',
            field=models.BigIntegerField(db_index=True, default=0),
        ),
    ]
