# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-08 12:08
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0036_merge_20190205_1740'),
    ]

    operations = [
        migrations.AddField(
            model_name='adcreation',
            name='description_1',
            field=models.CharField(default=None, max_length=250, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='description_2',
            field=models.CharField(default=None, max_length=250, null=True),
        ),
        migrations.AddField(
            model_name='adcreation',
            name='headline',
            field=models.CharField(default=None, max_length=250, null=True),
        ),
    ]
