# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-06-02 14:28
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0007_auto_20170530_0820"),
    ]

    operations = [
        migrations.AddField(
            model_name="account",
            name="updated_date",
            field=models.DateField(auto_now_add=True, default="2016-01-01"),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="campaign",
            name="updated_date",
            field=models.DateField(auto_now_add=True, default="2016-01-01"),
            preserve_default=False,
        ),
    ]
