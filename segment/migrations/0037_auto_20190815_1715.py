# -*- coding: utf-8 -*-
# Generated by Django 1.11.17 on 2019-08-15 17:15
from __future__ import unicode_literals

from django.db import migrations, models
import uuid


# Unable to use default uuid value as during migration, uuid value is calculated only once and will
# result in an IntegrityError
# https://docs.djangoproject.com/en/1.11/howto/writing-migrations/#migrations-that-add-unique-fields
def create_uuid(apps, schema_editor):
    CustomSegment = apps.get_model("segment", "CustomSegment")
    PersistentSegmentChannel = apps.get_model("segment", "PersistentSegmentChannel")
    PersistentSegmentVideo = apps.get_model("segment", "PersistentSegmentVideo")
    segment_models = [CustomSegment, PersistentSegmentChannel, PersistentSegmentVideo]

    for model in segment_models:
        for segment in model.objects.all():
            segment.uuid = uuid.uuid4()
            segment.save(update_fields=['uuid'])


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0036_auto_20190806_1706'),
    ]

    operations = [
        migrations.AddField(
            model_name='customsegment',
            name='uuid',
            field=models.UUIDField(null=True),
        ),
        migrations.AddField(
            model_name='persistentsegmentchannel',
            name='uuid',
            field=models.UUIDField(null=True),
        ),
        migrations.AddField(
            model_name='persistentsegmentvideo',
            name='uuid',
            field=models.UUIDField(null=True),
        ),

        migrations.RunPython(create_uuid),

        migrations.AlterField(
            model_name='customsegment',
            name='uuid',
            field=models.UUIDField(unique=True),
        ),
        migrations.AlterField(
            model_name='persistentsegmentchannel',
            name='uuid',
            field=models.UUIDField(unique=True),
        ),
        migrations.AlterField(
            model_name='persistentsegmentvideo',
            name='uuid',
            field=models.UUIDField(unique=True),
        ),
    ]

