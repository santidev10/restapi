# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-23 16:41
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0022_auto_20190429_1651'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditchannelmeta',
            name='video_count',
            field=models.BigIntegerField(db_index=True, default=None, null=True),
        ),
    ]
