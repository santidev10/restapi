# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-10-10 15:28
from __future__ import unicode_literals

import django.contrib.postgres.fields
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0008_auto_20170616_1335'),
    ]

    operations = [
        migrations.AddField(
            model_name='segmentchannel',
            name='top_recommend_channels',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=60), default=list,
                                                            size=None),
        ),
    ]
