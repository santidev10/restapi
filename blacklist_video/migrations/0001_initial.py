# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-02-21 00:55
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='BlacklistVideo',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('video_id', models.CharField(max_length=225)),
                ('channel_id', models.CharField(max_length=225)),
                ('channel_title', models.CharField(max_length=225)),
                ('title', models.CharField(max_length=225)),
                ('description', models.TextField()),
                ('scanned', models.BooleanField(default=False)),
            ],
        ),
    ]
