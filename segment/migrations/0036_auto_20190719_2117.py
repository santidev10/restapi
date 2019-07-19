# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-07-19 21:17
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0035_auto_20190719_2001'),
    ]

    operations = [
        migrations.AlterField(
            model_name='persistentsegmentchannel',
            name='category',
            field=models.CharField(db_index=True, default='whitelist', max_length=255),
        ),
        migrations.AlterField(
            model_name='persistentsegmentchannel',
            name='is_master',
            field=models.BooleanField(db_index=True),
        ),
        migrations.AlterField(
            model_name='persistentsegmentvideo',
            name='category',
            field=models.CharField(db_index=True, default='whitelist', max_length=255),
        ),
        migrations.AlterField(
            model_name='persistentsegmentvideo',
            name='is_master',
            field=models.BooleanField(db_index=True),
        ),
    ]
