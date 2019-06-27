# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-17 23:37
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('brand_safety', '0015_auto_20190617_1808'),
    ]

    operations = [
        migrations.AddField(
            model_name='badwordhistory',
            name='after',
            field=models.CharField(db_index=True, default='', max_length=180),
        ),
        migrations.AddField(
            model_name='badwordhistory',
            name='before',
            field=models.CharField(db_index=True, default='', max_length=180),
        ),
        migrations.AddField(
            model_name='badwordhistory',
            name='fields_modified',
            field=models.CharField(db_index=True, default='', max_length=80),
        ),
    ]