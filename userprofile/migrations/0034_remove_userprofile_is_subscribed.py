# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-10-30 14:59
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0033_userprofile_synced_with_email_campaign'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='userprofile',
            name='is_subscribed',
        ),
    ]
