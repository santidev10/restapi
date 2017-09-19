# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-07-14 08:21
from __future__ import unicode_literals

import datetime
from django.db import migrations, models
import django.db.models.deletion
from django.utils.timezone import utc


class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0010_auto_20170713_0847'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='accountcreation',
            name='is_changed',
        ),
        migrations.RemoveField(
            model_name='accountcreation',
            name='version',
        ),
        migrations.AddField(
            model_name='accountcreation',
            name='is_managed',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='accountcreation',
            name='sync_at',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=datetime.datetime(2017, 7, 14, 8, 20, 54, 326745, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='adcreation',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, default=datetime.datetime(2017, 7, 14, 8, 21, 2, 825658, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='adgroupcreation',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=datetime.datetime(2017, 7, 14, 8, 21, 13, 591985, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='adgroupcreation',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, default=datetime.datetime(2017, 7, 14, 8, 21, 21, 295700, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='campaigncreation',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=datetime.datetime(2017, 7, 14, 8, 21, 36, 5615, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='campaigncreation',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, default=datetime.datetime(2017, 7, 14, 8, 21, 38, 400002, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='accountcreation',
            name='account',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='account_creations', to='aw_reporting.Account'),
        ),
    ]
