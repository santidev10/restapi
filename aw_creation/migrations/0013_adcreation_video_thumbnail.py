# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-07-21 13:44
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0012_auto_20170717_1128'),
    ]

    operations = [
        migrations.AddField(
            model_name='adcreation',
            name='video_thumbnail',
            field=models.ImageField(blank=True, null=True, upload_to='img/brands'),
        ),
    ]
