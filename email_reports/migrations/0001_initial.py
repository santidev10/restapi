# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-10 11:37
from __future__ import unicode_literals

from django.db import migrations, models

import email_reports.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='SavedEmail',
            fields=[
                ('id', models.CharField(default=email_reports.models.get_uid, editable=False, max_length=32, primary_key=True, serialize=False)),
                ('html', models.TextField()),
                ('date', models.DateField(auto_now_add=True)),
            ],
        ),
    ]