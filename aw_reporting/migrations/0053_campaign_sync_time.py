# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-03-15 18:43
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0052_flight_budget"),
    ]

    operations = [
        migrations.AddField(
            model_name="campaign",
            name="sync_time",
            field=models.DateTimeField(null=True),
        ),
    ]
