# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-03-07 18:54
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0051_merge_20190205_1602"),
    ]

    operations = [
        migrations.AddField(
            model_name="flight",
            name="budget",
            field=models.FloatField(null=True),
        ),
    ]
