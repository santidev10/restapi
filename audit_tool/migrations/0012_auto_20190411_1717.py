# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-11 17:17
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0011_auto_20190410_1816'),
    ]

    operations = [
        migrations.AlterField(
            model_name='auditchannelmeta',
            name='description',
            field=models.TextField(default=None, null=True),
        ),
        migrations.AlterField(
            model_name='auditchannelmeta',
            name='keywords',
            field=models.TextField(default=None, null=True),
        ),
    ]
