# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-25 14:27
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0020_globalpermission'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='subscription',
            name='plan',
        ),
        migrations.RemoveField(
            model_name='subscription',
            name='user',
        ),
        migrations.RemoveField(
            model_name='userprofile',
            name='access',
        ),
        migrations.RemoveField(
            model_name='userprofile',
            name='permissions',
        ),
        migrations.RemoveField(
            model_name='userprofile',
            name='plan',
        ),
        migrations.DeleteModel(
            name='Plan',
        ),
        migrations.DeleteModel(
            name='Subscription',
        ),
    ]
