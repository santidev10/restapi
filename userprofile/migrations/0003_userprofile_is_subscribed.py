# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-09-14 10:08
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0002_auto_20170505_1605'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofile',
            name='is_subscribed',
            field=models.BooleanField(default=False),
        ),
    ]
