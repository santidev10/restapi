# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-18 22:13
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('brand_safety', '0017_auto_20190618_2013'),
    ]

    operations = [
        migrations.AlterField(
            model_name='badwordhistory',
            name='action',
            field=models.IntegerField(db_column='action', default=1),
        ),
    ]
