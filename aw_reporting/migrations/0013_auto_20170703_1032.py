# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-07-03 10:32
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0012_auto_20170703_0835"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="awconnectiontouserrelation",
            unique_together=set([("user", "connection")]),
        ),
    ]
