# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-01-08 10:44
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0047_update_territory'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='opportunity',
            name='bill_of_third_party_numbers',
        ),
        migrations.AddField(
            model_name='opportunity',
            name='billing_server',
            field=models.CharField(max_length=30, null=True),
        ),
    ]
