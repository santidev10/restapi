# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-01-24 17:26
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0024_auto_20190121_1700'),
    ]

    operations = [
        migrations.AddField(
            model_name='persistentsegmentchannel',
            name='category',
            field=models.CharField(default='whitelist', max_length=255),
        ),
        migrations.AddField(
            model_name='persistentsegmentvideo',
            name='category',
            field=models.CharField(default='whitelist', max_length=255),
        ),
    ]