# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-01-21 17:00
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0023_auto_20190121_1659'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='persistentsegmentchannel',
            name='shared_with',
        ),
        migrations.RemoveField(
            model_name='persistentsegmentvideo',
            name='shared_with',
        ),
    ]
