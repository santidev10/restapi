"""
BasePersistentSegment models module
"""
import logging

from django.conf import settings
from django.db.models import BooleanField
from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import IntegerField
from django.db.models import JSONField
from django.db.models import Manager
from django.db.models import Model
from django.db.models import TextField
from django.db.models import UUIDField
from django.utils import timezone

from audit_tool.models import AuditCategory
from segment.models.utils.segment_exporter import SegmentExporter
from utils.models import Timestampable
from .constants import PersistentSegmentCategory
from .constants import S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN
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
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1

    uuid = UUIDField(unique=True)
    title = CharField(max_length=255, null=True, blank=True)
    category = CharField(max_length=255, null=False, default=PersistentSegmentCategory.WHITELIST, db_index=True)
    audit_category_id = IntegerField(null=True, blank=True)
    is_master = BooleanField(default=False, db_index=True)

    details = JSONField(default=dict)

    related = None  # abstract property
    segment_type = None  # abstract property
    files = None  # abstract property

    export_content_type = "application/CSV"
    export_last_modified = None

    thumbnail_image_url = TextField(default="")

    class Meta:
        abstract = True
        ordering = ["pk"]

    @property
    def s3(self):
        try:
            self._s3
        except AttributeError:
            self._s3 = SegmentExporter(self, bucket_name=settings.AMAZON_S3_BUCKET_NAME)
        return self._s3

    def export_file(self, queryset=None, filename=None):
        now = timezone.now()
        s3_key = self.get_s3_key(datetime=now)
        if filename:
            self.s3.export_file_to_s3(filename, s3_key)
        else:
            self.s3.export_to_s3(self, s3_key, queryset=queryset)
        PersistentSegmentFileUpload.objects.create(segment_uuid=self.uuid, filename=s3_key, created_at=now)

    @property
    def audit_category(self):
        if self.audit_category_id:
            return AuditCategory.objects.get(id=self.audit_category_id)
        return None

    @audit_category.setter
    def audit_category(self, audit_category):
        if audit_category and audit_category.id:
            self.audit_category_id = audit_category.id

    # pylint: disable=signature-differs
    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        # Method provided by segment_mixin
        self.remove_all_from_segment()
        super().delete(*args, **kwargs)
        return self
    # pylint: enable=signature-differs

    def get_s3_key(self, from_db=False, datetime=None):
        try:
            # Get latest filename from db to retrieve from s3
            if from_db is True:
                latest_filename = \
                PersistentSegmentFileUpload.objects.filter(segment_uuid=self.uuid).order_by("-created_at")[0].filename
                return latest_filename
            # Get new filename to upload using date string
            key = S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type,
                                                                    segment_title=self.title,
                                                                    datetime=datetime or timezone.now())
        except IndexError:
            key = S3_SEGMENT_EXPORT_KEY_PATTERN.format(segment_type=self.segment_type, segment_title=self.title)
        return key


class BasePersistentSegmentRelated(Timestampable):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=100, db_index=True)
    category = CharField(max_length=100, default="")
    title = TextField(default="")
    thumbnail_image_url = TextField(default="")

    details = JSONField(default=dict)

    class Meta:
        abstract = True
        unique_together = (
            ("segment", "related_id"),
        )


class PersistentSegmentFileUpload(Model):
    segment_uuid = UUIDField(unique=True)
    created_at = DateTimeField(db_index=True, auto_now_add=True)
    filename = CharField(max_length=200, unique=True)
