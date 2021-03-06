# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-07-26 10:59
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0013_adcreation_video_thumbnail'),
    ]

    operations = [
        migrations.AddField(
            model_name='adcreation',
            name='companion_banner',
            field=models.ImageField(blank=True, null=True, upload_to='img/custom_video_thumbs'),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='video_channel_title',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='video_description',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='video_id',
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='video_title',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
        migrations.AlterField(
            model_name='adcreation',
            name='video_thumbnail',
            field=models.URLField(blank=True, null=True),
        ),
    ]
