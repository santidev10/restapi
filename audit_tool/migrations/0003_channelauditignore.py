# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-11 16:16
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0002_auto_20180822_0402'),
    ]

    operations = [
        migrations.CreateModel(
            name='ChannelAuditIgnore',
            fields=[
                ('id', models.CharField(db_index=True, max_length=30, primary_key=True, serialize=False)),
            ],
        ),
    ]
