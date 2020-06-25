# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-05-17 14:52
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0032_campaign_index_by_placement_code"),
    ]

    operations = [
        migrations.AlterField(
            model_name="campaign",
            name="placement_code",
            field=models.CharField(default=None, max_length=10, null=True),
        ),
        migrations.AlterField(
            model_name="opplacement",
            name="number",
            field=models.CharField(db_index=True, max_length=10, null=True),
        ),
    ]
