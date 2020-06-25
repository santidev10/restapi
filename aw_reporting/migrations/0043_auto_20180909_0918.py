# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-09-09 09:18
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0042_auto_20180909_0916"),
    ]

    operations = [
        migrations.AddField(
            model_name="keywordstatistic",
            name="clicks_app_store",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="keywordstatistic",
            name="clicks_call_to_action_overlay",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="keywordstatistic",
            name="clicks_cards",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="keywordstatistic",
            name="clicks_end_cap",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="keywordstatistic",
            name="clicks_website",
            field=models.IntegerField(default=0),
        ),
    ]
