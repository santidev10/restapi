# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-03-04 16:33
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0025_auto_20190124_1726'),
    ]

    operations = [
        migrations.AddField(
            model_name='segmentchannel',
            name='pending_updates',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='segmentkeyword',
            name='pending_updates',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='segmentvideo',
            name='pending_updates',
            field=models.IntegerField(default=0),
        ),
    ]
