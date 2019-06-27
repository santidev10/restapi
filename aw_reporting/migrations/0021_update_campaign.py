# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-05 16:11
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0020_add_campaign_statistics'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaign',
            name='goal_allocation',
            field=models.FloatField(default=0),
        ),
        migrations.AlterField(
            model_name='campaign',
            name='account',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='campaigns', to='aw_reporting.Account'),
        ),
    ]