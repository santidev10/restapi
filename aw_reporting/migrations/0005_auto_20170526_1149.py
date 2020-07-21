# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-05-26 11:49
from __future__ import unicode_literals

import django.db.models.deletion
from django.conf import settings
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("aw_reporting", "0004_load_audience_topic"),
    ]

    operations = [
        migrations.CreateModel(
            name="AWAccountPermission",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("can_read", models.BooleanField(default=False)),
                ("can_write", models.BooleanField(default=False)),
                ("account",
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="mcc_permissions",
                                   to="aw_reporting.Account")),
            ],
        ),
        migrations.CreateModel(
            name="AWConnection",
            fields=[
                ("email", models.EmailField(max_length=254, primary_key=True, serialize=False)),
                ("refresh_token", models.CharField(max_length=150)),
                ("users", models.ManyToManyField(related_name="aw_connections", to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.RemoveField(
            model_name="accountconnection",
            name="manager",
        ),
        migrations.RemoveField(
            model_name="accountconnection",
            name="user",
        ),
        migrations.DeleteModel(
            name="AccountConnection",
        ),
        migrations.AddField(
            model_name="awaccountpermission",
            name="aw_connection",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="mcc_permissions",
                                    to="aw_reporting.AWConnection"),
        ),
        migrations.AlterUniqueTogether(
            name="awaccountpermission",
            unique_together=set([("aw_connection", "account")]),
        ),
    ]
