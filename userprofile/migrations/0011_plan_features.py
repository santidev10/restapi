# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-11-21 10:16
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0010_auto_20171118_1049'),
    ]

    operations = [
        migrations.AddField(
            model_name='plan',
            name='features',
            field=django.contrib.postgres.fields.jsonb.JSONField(default=[]),
        ),
    ]
