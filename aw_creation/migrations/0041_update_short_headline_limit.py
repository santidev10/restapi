# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-22 13:43
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0040_auto_20190213_2011'),
    ]

    operations = [
        migrations.AlterField(
            model_name='adcreation',
            name='short_headline',
            field=models.CharField(blank=True, max_length=25, null=True),
        ),
    ]
