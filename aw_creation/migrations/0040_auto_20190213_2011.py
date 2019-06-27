# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-13 20:11
from __future__ import unicode_literals

from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0039_auto_20190212_0651'),
    ]

    operations = [
        migrations.AddField(
            model_name='adcreation',
            name='business_name',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='long_headline',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='short_headline',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='video_ad_format',
            field=models.CharField(choices=[('TRUE_VIEW_IN_STREAM', 'In-stream'), ('TRUE_VIEW_IN_DISPLAY', 'Discovery'), ('BUMPER', 'Bumper'), ('DISPLAY', 'Display')], default='TRUE_VIEW_IN_STREAM', max_length=20),
        ),
    ]