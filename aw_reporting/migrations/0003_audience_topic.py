# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-05-15 13:49
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Audience",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=150)),
                ("type", models.CharField(db_index=True, max_length=25)),
                ("parent",
                 models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name="children",
                                   to="aw_reporting.Audience")),
            ],
        ),
        migrations.CreateModel(
            name="Topic",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(db_index=True, max_length=150)),
                ("parent",
                 models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name="children",
                                   to="aw_reporting.Topic")),
            ],
        ),
    ]
