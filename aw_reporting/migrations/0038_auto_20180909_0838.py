# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-09-09 08:38
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0037_merge_20180907_1430"),
    ]

    operations = [
        migrations.AddField(
            model_name="adgroupstatistic",
            name="clicks_app_store",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="adgroupstatistic",
            name="clicks_call_to_action_overlay",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="adgroupstatistic",
            name="clicks_cards",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="adgroupstatistic",
            name="clicks_end_cap",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="adgroupstatistic",
            name="clicks_website",
            field=models.IntegerField(default=0),
        ),
    ]
