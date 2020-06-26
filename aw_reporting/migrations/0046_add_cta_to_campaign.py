# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-10-24 18:33
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0045_auto_20180909_0919"),
    ]

    operations = [
        migrations.AddField(
            model_name="campaign",
            name="clicks_app_store",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaign",
            name="clicks_call_to_action_overlay",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaign",
            name="clicks_cards",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaign",
            name="clicks_end_cap",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaign",
            name="clicks_website",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaignstatistic",
            name="clicks_app_store",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaignstatistic",
            name="clicks_call_to_action_overlay",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaignstatistic",
            name="clicks_cards",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaignstatistic",
            name="clicks_end_cap",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="campaignstatistic",
            name="clicks_website",
            field=models.IntegerField(default=0),
        ),
    ]
