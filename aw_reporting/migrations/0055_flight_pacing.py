# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-02 20:27
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0054_auto_20190326_2013"),
    ]

    operations = [
        migrations.AddField(
            model_name="flight",
            name="pacing",
            field=models.FloatField(null=True),
        ),
    ]
