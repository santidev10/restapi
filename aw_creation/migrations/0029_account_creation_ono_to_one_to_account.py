# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-08-21 11:54
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0028_account_creation_cleanup'),
    ]

    operations = [
        migrations.AlterField(
            model_name='accountcreation',
            name='account',
            field=models.OneToOneField(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE,
                                       related_name='account_creation', to='aw_reporting.Account'),
        ),
    ]