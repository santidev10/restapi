# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-09-13 08:35
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0020_auto_20170830_1341'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='campaigncreation',
            name='video_ad_format',
        ),
        migrations.AddField(
            model_name='adgroupcreation',
            name='video_ad_format',
            field=models.CharField(
                choices=[('TRUE_VIEW_IN_STREAM', 'In-stream'), ('TRUE_VIEW_IN_DISPLAY', 'Discovery'),
                         ('BUMPER', 'Bumper')], default='TRUE_VIEW_IN_STREAM', max_length=20),
        ),
        migrations.AddField(
            model_name='campaigncreation',
            name='bid_strategy_type',
            field=models.CharField(choices=[('CPV', 'CPV'), ('CPM', 'CPM')], default='CPV', max_length=3),
        ),
    ]
