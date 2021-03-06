# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-07-03 08:35
from __future__ import unicode_literals

import datetime

import django.db.models.deletion
from django.conf import settings
from django.db import migrations
from django.db import models
from django.utils.timezone import utc


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("aw_reporting", "0011_auto_20170615_1000"),
    ]

    operations = [
        migrations.CreateModel(
            name="AWConnectionToUserRelation",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created", models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.RemoveField(
            model_name="account",
            name="updated_date",
        ),
        migrations.RemoveField(
            model_name="awconnection",
            name="users",
        ),
        migrations.RemoveField(
            model_name="campaign",
            name="updated_date",
        ),
        migrations.AddField(
            model_name="account",
            name="update_time",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name="campaign",
            name="update_time",
            field=models.DateTimeField(auto_now_add=True,
                                       default=datetime.datetime(2017, 7, 3, 8, 35, 33, 915554, tzinfo=utc)),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="awconnectiontouserrelation",
            name="connection",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="user_relations",
                                    to="aw_reporting.AWConnection"),
        ),
        migrations.AddField(
            model_name="awconnectiontouserrelation",
            name="user",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="aw_connections",
                                    to=settings.AUTH_USER_MODEL),
        ),
    ]
