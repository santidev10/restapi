# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-24 17:31
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0030_auto_20190621_0039'),
    ]

    operations = [
        migrations.AddField(
            model_name='customsegment',
            name='title_hash',
            field=models.BigIntegerField(db_index=True, default=0),
        ),
        migrations.AlterUniqueTogether(
            name='customsegment',
            unique_together=set([]),
        ),
    ]