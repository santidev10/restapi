"""
BasePersistentSegment models module
"""
import csv
import logging
import os
import tempfile

import boto3
from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BooleanField
from django.db.models import CharField
from django.db.models import Manager
from django.db.models import TextField
from django.db.models import BigIntegerField
from django.db.models import DateTimeField
from django.db.models import Model
from django.contrib.postgres.fields.jsonb import KeyTransform
from django.db.models import UUIDField

from utils.models import Timestampable
from .constants import PersistentSegmentCategory
from .constants import S3_SEGMENT_EXPORT_KEY_PATTERN
from .constants import S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN
from es_components.query_builder import QueryBuilder
from es_components.constants import SEGMENTS_UUID_FIELD

logger = logging.getLogger(__name__)


class PersistentSegmentManager(Manager):
    """
    Extend default persistent segment manager
    """


class BasePersistentSegment(Timestampable):
    """
    Base persistent segment model
    """
    uuid = UUIDField(unique=True)
    title = CharField(max_length=255, null=True, blank=True)
    category = CharField(max_length=255, null=False, default=PersistentSegmentCategory.WHITELIST, db_index=True)
    is_master = BooleanField(default=False, db_index=True)

    details = JSONField(default=dict())

    related = None  # abstract property
    segment_type = None  # abstract property
    files = None # abstract property

    export_content_type = "application/CSV"
    export_last_modified = None

    thumbnail_image_url = TextField(default="")

    class Meta:
        abstract = True
        ordering = ["pk"]

    def calculate_details(self):
        raise NotImplementedError

    def get_s3_key(self, from_db=False, datetime=None):
        try:
            # Get latest filename from db to retrieve from s3
            if from_db is True:
                latest_filename = PersistentSegmentFileUpload.objects.filter(segment_id=self.id).order_by("-created_at")[0].filename
                return latest_filename
            else:
                # Get new filename to upload using date string
                key = S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type, segment_title=self.title, datetime=datetime)
        except IndexError:
            key = S3_SEGMENT_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type, segment_title=self.title)
        return key

    def _s3(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY
        )
        return s3

    def export_to_s3(self, s3_key):
        with PersistentSegmentExportContent(segment=self) as exported_file_name:
            self._s3().upload_file(
                Bucket=settings.AMAZON_S3_BUCKET_NAME,
                Key=s3_key,
                Filename=exported_file_name,
            )

    def get_s3_export_content(self):
        s3 = self._s3()
        # Get latest entry from file upload manager
        try:
            key = self.get_s3_key(from_db=True)
            s3_object = s3.get_object(
                Bucket=settings.AMAZON_S3_BUCKET_NAME,
                Key=key
            )
            self.s3_filename = key
        except s3.exceptions.NoSuchKey:
            raise self.DoesNotExist
        body = s3_object.get("Body")
        self.export_last_modified = s3_object.get("LastModified")
        return body

    def get_segment_items(self):
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        return query

    def extract_aggregations(self, aggregation_result_dict):
        """
        Extract value fields of aggregation results
        :param aggregation_result_dict: { "agg_name" : { value: "a_value" } }
        :return:
        """
        results = {}
        for key, value in aggregation_result_dict:
            results[key] = value["value"]
        return results


class BasePersistentSegmentRelated(Timestampable):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=100, db_index=True)
    category = CharField(max_length=100, default="")
    title = TextField(default="")
    thumbnail_image_url = TextField(default="")

    details = JSONField(default=dict())

    class Meta:
        abstract = True
        unique_together = (
            ("segment", "related_id"),
        )


class PersistentSegmentExportContent(object):
    CHUNK_SIZE = 1000

    def __init__(self, segment):
        self.segment = segment

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)

        with open(self.filename, mode="w+", newline="") as export_file:
            queryset = self.segment.get_queryset()
            field_names = self.segment.get_export_columns()
            writer = csv.DictWriter(export_file, fieldnames=field_names)
            writer.writeheader()
            page = 0
            while True:
                offset = page * self.CHUNK_SIZE
                limit = (page + 1) * self.CHUNK_SIZE
                items = queryset[offset:limit]
                page += 1
                rows = self.segment.export_serializer(items, many=True).data
                if not rows:
                    break
                writer.writerows(rows)
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)

    def _data_generator(self, export_serializer, queryset):
        for item in queryset:
            data = export_serializer(item).data
            yield data


class PersistentSegmentFileUpload(Model):
    segment_id = BigIntegerField()
    created_at = DateTimeField(db_index=True)
    filename = CharField(max_length=200, unique=True)
