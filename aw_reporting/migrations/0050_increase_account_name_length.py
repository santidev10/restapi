# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-04 11:31
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0049_merge_20190103_1316"),
    ]

    operations = [
        migrations.AlterField(
            model_name="account",
            name="name",
            field=models.CharField(max_length=255, null=True),
        ),
    ]
