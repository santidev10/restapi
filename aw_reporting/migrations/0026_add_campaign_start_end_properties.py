# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-22 14:44
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0025_add_campaign_geo_targeting"),
    ]

    operations = [
        migrations.AddField(
            model_name="campaign",
            name="_end",
            field=models.DateField(null=True),
        ),
        migrations.AddField(
            model_name="campaign",
            name="_start",
            field=models.DateField(null=True),
        ),
    ]
