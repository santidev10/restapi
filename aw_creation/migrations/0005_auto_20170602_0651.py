# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-06-02 06:51
from __future__ import unicode_literals

import django.core.validators
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0004_auto_20170522_1447'),
    ]

    operations = [
        migrations.AlterField(
            model_name='frequencycap',
            name='limit',
            field=models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)]),
        ),
    ]
