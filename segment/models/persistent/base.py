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
from django.db.models import CharField
from django.db.models import Manager
from django.db.models import TextField

from utils.models import Timestampable
from .constants import PersistentSegmentCategory
from .constants import PersistentSegmentExportColumn
from .constants import S3_SEGMENT_EXPORT_KEY_PATTERN

logger = logging.getLogger(__name__)


class PersistentSegmentManager(Manager):
    """
    Extend default persistent segment manager
    """


class BasePersistentSegment(Timestampable):
    """
    Base persistent segment model
    """
    title = CharField(max_length=255, null=True, blank=True)
    category = CharField(max_length=255, null=False, default=PersistentSegmentCategory.WHITELIST)

    details = JSONField(default=dict())

    related = None  # abstract property
    segment_type = None  # abstract property

    export_content_type = "application/CSV"
    export_last_modified = None

    thumbnail_image_url = TextField(default="")

    class Meta:
        abstract = True
        ordering = ["pk"]

    def calculate_details(self):
        raise NotImplementedError

    def get_s3_key(self):
        key = S3_SEGMENT_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type, segment_title=self.title)
        return key

    def get_export_columns(self):
        if self.segment_type is None:
            raise ValueError("Undefined segment type")

        if self.category is None:
            raise ValueError("Undefined segment category")

        map_by_category = dict(PersistentSegmentExportColumn.CSV_COLUMNS_MAPS_BY_TYPE).get(self.segment_type)
        if map_by_category is None:
            raise ValueError("Unsupported segment type")

        export_columns = dict(map_by_category).get(self.category)
        if export_columns is None:
            raise ValueError("Unsupported segment category")

        return export_columns

    def _s3(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY
        )
        return s3

    def export_to_s3(self):
        with PersistentSegmentExportContent(segment=self) as exported_file_name:
            self._s3().upload_file(
                Bucket=settings.AMAZON_S3_BUCKET_NAME,
                Key=self.get_s3_key(),
                Filename=exported_file_name,
            )

    def get_s3_export_content(self):
        s3 = self._s3()
        try:
            s3_object = s3.get_object(
                Bucket=settings.AMAZON_S3_BUCKET_NAME,
                Key=self.get_s3_key()
            )
        except s3.exceptions.NoSuchKey:
            raise self.DoesNotExist
        body = s3_object.get("Body")
        self.export_last_modified = s3_object.get("LastModified")
        return body


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
    CHUNK_SIZE = 100000

    def __init__(self, segment):
        self.segment = segment

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)

        with open(self.filename, mode="w+", newline="") as export_file:
            queryset = self.segment.related.order_by("pk").all()
            field_names = self.segment.get_export_columns()
            writer = csv.DictWriter(export_file, fieldnames=field_names)
            writer.writeheader()
            page = 0
            while True:
                offset = page * self.CHUNK_SIZE
                limit = (page + 1) * self.CHUNK_SIZE
                items = queryset[offset:limit]
                page += 1

                rows = [
                    {key: value for key, value in item.get_exportable_row().items() if key in field_names}
                    for item in items
                ]
                if not rows:
                    break

                writer.writerows(rows)
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)
