# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-24 18:24
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0017_auditchannelmeta_view_count'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditprocessor',
            name='cached_data',
            field=django.contrib.postgres.fields.jsonb.JSONField(default={}),
        ),
    ]
