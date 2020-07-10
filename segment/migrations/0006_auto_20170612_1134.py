# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-06-12 11:34
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('segment', '0005_auto_20170525_1412'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='segment',
            name='channels',
        ),
        migrations.RemoveField(
            model_name='segment',
            name='owner',
        ),
        migrations.RemoveField(
            model_name='segment',
            name='videos',
        ),
        migrations.DeleteModel(
            name='ChannelRelation',
        ),
        migrations.DeleteModel(
            name='Segment',
        ),
        migrations.DeleteModel(
            name='VideoRelation',
        ),
    ]
