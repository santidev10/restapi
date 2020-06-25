# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-01 18:17
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('brand_safety', '0004_badchannel_is_deleted'),
    ]

    operations = [
        migrations.CreateModel(
            name='BadWordCategory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=80, unique=True)),
            ],
        ),
        migrations.AddField(
            model_name='badword',
            name='category_ref',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to='brand_safety.BadWordCategory'),
        ),
    ]
