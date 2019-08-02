# -*- coding: utf-8 -*-
# Generated by Django 1.11.17 on 2019-08-02 00:01
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0035_auto_20190719_2125'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='segmentchannel',
            name='owner',
        ),
        migrations.RemoveField(
            model_name='segmentkeyword',
            name='owner',
        ),
        migrations.AlterUniqueTogether(
            name='segmentrelatedchannel',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='segmentrelatedchannel',
            name='segment',
        ),
        migrations.AlterUniqueTogether(
            name='segmentrelatedkeyword',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='segmentrelatedkeyword',
            name='segment',
        ),
        migrations.AlterUniqueTogether(
            name='segmentrelatedvideo',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='segmentrelatedvideo',
            name='segment',
        ),
        migrations.RemoveField(
            model_name='segmentvideo',
            name='owner',
        ),
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
