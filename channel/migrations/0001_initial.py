# -*- coding: utf-8 -*-
# Generated by Django 1.11.17 on 2019-08-27 10:00
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='AuthChannel',
            fields=[
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('channel_id', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('user_info', models.TextField(null=True)),
                ('refresh_token', models.CharField(max_length=255, null=True)),
                ('access_token', models.CharField(max_length=255, null=True)),
                ('client_id', models.CharField(max_length=255)),
                ('client_secret', models.CharField(max_length=255)),
                ('access_token_expire_at', models.DateTimeField(null=True)),
            ],
            options={
                'db_table': 'auth_channel',
            },
        ),
    ]
