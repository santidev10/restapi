# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-10-18 06:43
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0002_auto_20170505_1605'),
    ]

    operations = [
        migrations.CreateModel(
            name='Plan',
            fields=[
                ('name', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('permissions', django.contrib.postgres.fields.jsonb.JSONField(default={'channel': {'audience': False, 'details': False, 'filter': False, 'list': False}, 'keyword': {'details': False, 'list': False}, 'segment': {'channel': {'all': False, 'private': True}, 'keyword': {'all': False, 'private': True}, 'video': {'all': False, 'private': True}}, 'settings': {'billing': True, 'my_aw_accounts': False, 'my_yt_channels': True}, 'video': {'audience': False, 'details': False, 'filter': False, 'list': False}, 'view': {'benchmarks': False, 'create_and_manage_campaigns': False, 'highlights': False, 'performance': False, 'trends': False}})),
            ],
        ),
        migrations.AddField(
            model_name='userprofile',
            name='plan',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='userprofile.Plan'),
        ),
    ]
