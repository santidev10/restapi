# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-20 22:14
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('audit_tool', '0024_auditexporter'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditexporter',
            name='owner_email',
            field=models.EmailField(null=True, blank=True),
        ),
    ]
