"""
BasePersistentSegment models module
"""
import logging

import boto3
from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BooleanField
from django.db.models import CharField
from django.db.models import Manager
from django.db.models import TextField
from django.db.models import DateTimeField
from django.db.models import Model
from django.db.models import IntegerField
from django.db.models import UUIDField

from audit_tool.models import AuditCategory
from utils.models import Timestampable
from .constants import PersistentSegmentCategory
from .constants import S3_SEGMENT_EXPORT_KEY_PATTERN
from .constants import S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN
from segment.models.utils.calculate_segment_statistics import calculate_statistics
from segment.models.utils.export_context_manager import ExportContextManager


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
    files = None # abstract property
    related_aw_statistics_model = None # abstract property

    export_content_type = "application/CSV"
    export_last_modified = None

    thumbnail_image_url = TextField(default="")

    class Meta:
        abstract = True
        ordering = ["pk"]

    @property
    def audit_category(self):
        if self.audit_category_id:
            return AuditCategory.objects.get(id=self.audit_category_id)

    @audit_category.setter
    def audit_category(self, audit_category):
        if audit_category and audit_category.id:
            self.audit_category_id = audit_category.id

    def calculate_statistics(self):
        es_manager = self.get_es_manager()
        statistics = calculate_statistics(self.related_aw_statistics_model, self.segment_type, es_manager, self.get_segment_items_query())
        return statistics

    def get_es_manager(self):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        from segment.utils import retry_on_conflict
        # Delete segment references from Elasticsearch
        retry_on_conflict(self.remove_all_from_segment, retry_amount=self.REMOVE_FROM_SEGMENT_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFF)
        super().delete(*args, **kwargs)
        return self

    def get_s3_key(self, from_db=False, datetime=None):
        try:
            # Get latest filename from db to retrieve from s3
            if from_db is True:
                latest_filename = PersistentSegmentFileUpload.objects.filter(segment_uuid=self.uuid).order_by("-created_at")[0].filename
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
        with ExportContextManager(segment=self) as exported_file_name:
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

    # def get_segment_items_query(self):
    #     query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
    #     return query

    # def extract_aggregations(self, aggregation_result_dict):
    #     """
    #     Extract value fields of aggregation results
    #     :param aggregation_result_dict: { "agg_name" : { value: "a_value" } }
    #     :return:
    #     """
    #     results = {}
    #     for key, value in aggregation_result_dict.items():
    #         results[key] = value["value"]
    #     return results
    #
    # def remove_all_from_segment(self):
    #     """
    #     Remove all references to segment uuid from Elasticsearch
    #     :return:
    #     """
    #     es_manager = self.get_es_manager()
    #     query = self.get_segment_items_query()
    #     es_manager.remove_from_segment(query, self.uuid)


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
    created_at = DateTimeField(db_index=True)
    filename = CharField(max_length=200, unique=True)
