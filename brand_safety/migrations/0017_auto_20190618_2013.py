# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-18 20:13
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('brand_safety', '0016_auto_20190617_2337'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='badwordhistory',
            name='after',
        ),
        migrations.RemoveField(
            model_name='badwordhistory',
            name='before',
        ),
        migrations.RemoveField(
            model_name='badwordhistory',
            name='fields_modified',
        ),
        migrations.AddField(
            model_name='badwordhistory',
            name='changes',
            field=models.CharField(db_index=True, default='', max_length=250),
        ),
    ]
