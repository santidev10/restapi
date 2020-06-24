# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-15 21:55
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0013_auditcategory_category_display'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditprocessor',
            name='audit_type',
            field=models.IntegerField(db_index=True, default=0),
        ),
        migrations.AddField(
            model_name='auditvideoprocessor',
            name='clean',
            field=models.BooleanField(db_index=True, default=True),
        ),
    ]
