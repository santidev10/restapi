# -*- coding: utf-8 -*-
# Generated by Django 1.11.17 on 2019-08-06 17:06
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0035_auto_20190719_2125'),
    ]

    operations = [
        migrations.DeleteModel(
            name='SegmentChannel',
        ),
        migrations.DeleteModel(
            name='SegmentKeyword',
        ),
        migrations.DeleteModel(
            name='SegmentRelatedChannel',
        ),
        migrations.DeleteModel(
            name='SegmentRelatedKeyword',
        ),
        migrations.DeleteModel(
            name='SegmentRelatedVideo',
        ),
        migrations.DeleteModel(
            name='SegmentVideo',
        ),
    ]
