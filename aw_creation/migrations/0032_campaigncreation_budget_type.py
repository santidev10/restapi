# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-12-19 13:25
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0031_remove_optimization_models'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaigncreation',
            name='budget_type',
            field=models.CharField(choices=[('daily', 'daily'), ('total', 'total')], default='daily', max_length=30),
        ),
    ]
