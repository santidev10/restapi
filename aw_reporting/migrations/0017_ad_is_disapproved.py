# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-11-28 17:16
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0016_auto_20171120_1210"),
    ]

    operations = [
        migrations.AddField(
            model_name="ad",
            name="is_disapproved",
            field=models.BooleanField(default=False),
        ),
    ]
