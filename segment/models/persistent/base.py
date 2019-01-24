"""
BasePersistentSegment models module
"""
import boto3
import csv
from io import StringIO
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import CharField
from django.db.models import TextField
from django.db.models import Manager


from utils.models import Timestampable
from .constants import PersistentSegmentCategory
from ...names import S3_SEGMENT_EXPORT_KEY_PATTERN

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
    export_columns = None # abstract property
    export_last_modified = None

    class Meta:
        abstract = True
        ordering = ["pk"]

    def calculate_details(self):
        raise NotImplementedError

    def get_s3_key(self):
        key = S3_SEGMENT_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type, segment_title=self.title)
        return key

    def get_export_content(self):
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.export_columns)
        writer.writeheader()
        rows = [related.get_exportable_row() for related in self.related.all()]
        writer.writerows(rows)
        return output.getvalue()

    def _s3(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY
        )
        return s3

    def export_to_s3(self):
        self._s3().put_object(
            Bucket=settings.AMAZON_S3_BUCKET_NAME,
            Key=self.get_s3_key(),
            Body=self.get_export_content(),
            ContentType=self.export_content_type
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
