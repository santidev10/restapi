# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-07-18 19:11
from __future__ import unicode_literals

import django.db.models.deletion
from django.conf import settings
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0033_auto_20190708_2141'),
    ]

    operations = [
        migrations.AlterField(
            model_name='customsegment',
            name='owner',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to=settings.AUTH_USER_MODEL),
        ),
    ]
