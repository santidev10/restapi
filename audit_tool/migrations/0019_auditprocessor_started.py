# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-24 23:12
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0018_auditprocessor_cached_data'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditprocessor',
            name='started',
            field=models.DateTimeField(db_index=True, default=None, null=True),
        ),
    ]