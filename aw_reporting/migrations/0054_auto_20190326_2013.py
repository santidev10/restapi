# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-03-26 20:13
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0053_campaign_sync_time"),
    ]

    operations = [
        migrations.AddField(
            model_name="opportunity",
            name="cpm_buffer",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="cpv_buffer",
            field=models.IntegerField(default=0),
        ),
    ]
