# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-16 19:12
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations
import userprofile.models


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0039_update_demo_account'),
    ]

    operations = [
        migrations.AlterField(
            model_name='userprofile',
            name='aw_settings',
            field=django.contrib.postgres.fields.jsonb.JSONField(default=userprofile.models.get_default_settings),
        ),
    ]
