# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-04 11:31
from __future__ import unicode_literals

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0033_campaigncreation_is_draft'),
    ]

    operations = [
        migrations.AlterField(
            model_name='accountcreation',
            name='name',
            field=models.CharField(max_length=255, validators=[django.core.validators.RegexValidator("^[^#']*$", "# and ' are not allowed for titles")]),
        ),
    ]