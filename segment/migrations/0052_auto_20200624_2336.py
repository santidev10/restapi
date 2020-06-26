# Generated by Django 3.0.4 on 2020-06-24 23:36

from django.db import migrations
from segment.models import CustomSegment

def get_s3_key(segment):
    """gets s3 key for CustomSegmentFileUpload"""
    segment_type = CustomSegment.SEGMENT_TYPE_CHOICES[segment.segment_type][1]
    return f"custom_segments/{segment.owner_id}/{segment_type}/{segment.title}.csv"

def get_vetted_s3_key(segment, suffix=None):
    """
    gets s3 key for CustomSegmentVettedFileUpload
    seems like the suffix option hasn't been used for any of the existing records.
    suffix was being generated randomly with a uuid
    """
    suffix = suffix if suffix is not None else ""
    segment_type = segment.SEGMENT_TYPE_CHOICES[segment.segment_type][1]
    return f"custom_segments/{segment.owner_id}/{segment_type}/vetted/{segment.title}{suffix}.csv"

def persist_existing_s3_keys(apps, schema_editor):
    for model_name, old_s3_key_getter in {
        'CustomSegmentFileUpload': get_s3_key,
        'CustomSegmentVettedFileUpload': get_vetted_s3_key,
    }.items():
        model = apps.get_model('segment', model_name)
        migrate_s3_keys(model, old_s3_key_getter)

def migrate_s3_keys(model, old_s3_key_getter):
    query = model.objects.prefetch_related('segment')
    for file_upload_instance in query:
        if file_upload_instance.filename:
            continue
        s3_key = old_s3_key_getter(file_upload_instance.segment)
        file_upload_instance.filename = s3_key
        file_upload_instance.save(update_fields=['filename',])


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0051_auto_20200624_2149'),
    ]

    operations = [
        migrations.RunPython(persist_existing_s3_keys),
    ]
