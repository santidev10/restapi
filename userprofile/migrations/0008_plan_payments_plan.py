# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-11-17 16:01
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('payments', '0002_event'),
        ('userprofile', '0007_auto_20171110_0928'),
    ]

    operations = [
        migrations.AddField(
            model_name='plan',
            name='payments_plan',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='payments.Plan'),
        ),
    ]
