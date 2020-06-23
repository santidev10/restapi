# -*- coding: utf-8 -*-
# Generated by Django 1.11.17 on 2019-08-20 16:08
from __future__ import unicode_literals

from django.db import migrations
from django.db import models


def set_upload_uuid(apps, schema_editor):
    PersistentSegmentChannel = apps.get_model("segment", "PersistentSegmentChannel")
    PersistentSegmentVideo = apps.get_model("segment", "PersistentSegmentVideo")
    PersistentSegmentFileUpload = apps.get_model("segment", "PersistentSegmentFileUpload")

    for segment in PersistentSegmentChannel.objects.all():
        PersistentSegmentFileUpload.objects.filter(filename__icontains="channel", segment_id=segment.id).update(
            segment_uuid=segment.uuid)

    for segment in PersistentSegmentVideo.objects.all():
        PersistentSegmentFileUpload.objects.filter(filename__icontains="video", segment_id=segment.id).update(
            segment_uuid=segment.uuid)


class Migration(migrations.Migration):
    dependencies = [
        ('segment', '0039_auto_20190816_2237'),
    ]

    operations = [
        migrations.AddField(
            model_name='persistentsegmentfileupload',
            name='segment_uuid',
            field=models.UUIDField(null=True),
        ),

        migrations.RunPython(set_upload_uuid),

        migrations.RemoveField(
            model_name='persistentsegmentfileupload',
            name='segment_id',
        ),
        migrations.AlterField(
            model_name='persistentsegmentfileupload',
            name='segment_uuid',
            field=models.UUIDField(unique=True),
        ),
    ]
