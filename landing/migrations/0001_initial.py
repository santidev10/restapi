# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-06-22 15:03
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ContactMessage',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('subject', models.CharField(default='', max_length=255)),
                ('email', models.CharField(default='', max_length=255)),
                ('data', django.contrib.postgres.fields.jsonb.JSONField(default={})),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
