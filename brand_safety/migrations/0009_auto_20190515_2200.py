# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-15 22:00
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('brand_safety', '0008_auto_20190515_2100'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='badword',
            unique_together=set([('name', 'category_ref')]),
        ),
        migrations.RemoveField(
            model_name='badword',
            name='category',
        ),
    ]
