# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-20 20:54
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0027_auto_20190424_2156'),
    ]

    operations = [
        migrations.CreateModel(
            name='PersistentSegmentFileUpload',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('segment_id', models.BigIntegerField()),
                ('created_at', models.DateTimeField(db_index=True)),
                ('filename', models.CharField(max_length=200, unique=True)),
            ],
        ),
    ]
