# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-12-27 15:24
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0018_auto_20181227_0600'),
    ]

    operations = [
        migrations.AddField(
            model_name='persistentsegmentrelatedchannel',
            name='category',
            field=models.CharField(default='', max_length=100),
        ),
        migrations.AddField(
            model_name='persistentsegmentrelatedvideo',
            name='category',
            field=models.CharField(default='', max_length=100),
        ),
    ]
