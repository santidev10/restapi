# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-07-27 15:13
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0027_merge_20180716_1012'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofile',
            name='logo',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]