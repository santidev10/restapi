"""
BaseSegment models module
"""
import logging
from uuid import uuid4

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db import models

from audit_tool.models import AuditProcessor
from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.api.export_serializers import CustomSegmentChannelExportSerializer
from segment.api.export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.export_serializers import CustomSegmentVideoExportSerializer
from segment.api.export_serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.export_serializers import CustomSegmentVideoVettedExportSerializer
from segment.models.constants import CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY
from segment.models.constants import ChannelConfig
from segment.models.constants import VideoConfig
from segment.models.segment_mixin import SegmentMixin
from segment.models.utils.segment_audit_utils import SegmentAuditUtils
from segment.models.utils.segment_exporter import SegmentExporter
from utils.models import Timestampable

logger = logging.getLogger(__name__)


class CustomSegment(SegmentMixin, Timestampable):
    """
    Base segment model
    """
    export_content_type = "application/CSV"
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS,
                Sections.TASK_US_DATA, Sections.ADS_STATS, Sections.CUSTOM_PROPERTIES,)
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1
    is_vetting = False

    LIST_TYPE_CHOICES = (
        (0, WHITELIST),
        (1, BLACKLIST)
    )
    SEGMENT_TYPE_CHOICES = (
        (0, VIDEO),
        (1, CHANNEL)
    )
    segment_type_to_id = {
        segment_type: _id for _id, segment_type in dict(SEGMENT_TYPE_CHOICES).items()
    }
    list_type_to_id = {
        list_type: _id for _id, list_type in dict(LIST_TYPE_CHOICES).items()
    }
    segment_id_to_type = {
        _id: segment_type for segment_type, _id in segment_type_to_id.items()
    }
    list_id_to_type = {
        _id: list_type for list_type, _id in list_type_to_id.items()
    }

    audit_id = models.IntegerField(null=True, default=None, db_index=True)
    uuid = models.UUIDField(unique=True, default=uuid4)
    statistics = JSONField(default=dict)
    list_type = models.IntegerField(choices=LIST_TYPE_CHOICES, null=True, default=None)
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.CASCADE)
    segment_type = models.IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = models.CharField(max_length=255, db_index=True)
    title_hash = models.BigIntegerField(default=0, db_index=True)
    is_vetting_complete = models.BooleanField(default=False, db_index=True)
    is_featured = models.BooleanField(default=False, db_index=True)
    is_regenerating = models.BooleanField(default=False, db_index=True)
    featured_image_url = models.TextField(default="")

    @property
    def export_serializer(self):
        """ Get export serializer depending on channel or video segment """
        if self.segment_type in (0, "video"):
            serializer = self._get_video_serializer()
        else:
            serializer = self._get_channel_serializer()
        return serializer

    def _get_video_serializer(self):
        """ Get video export serializer depending on vetting """
        if self.is_vetting is True:
            serializer = CustomSegmentVideoVettedExportSerializer
        else:
            serializer = CustomSegmentVideoExportSerializer
        return serializer

    def _get_channel_serializer(self):
        """ Get channel export serializer depending on vetting and segment owner permissions """
        if self.is_vetting is True:
            serializer = CustomSegmentChannelVettedExportSerializer
        else:
            owner = getattr(self, "owner", None)
            if owner and owner.has_perm("userprofile.monetization_filter"):
                serializer = CustomSegmentChannelWithMonetizationExportSerializer
            else:
                serializer = CustomSegmentChannelExportSerializer
        return serializer

    @property
    def config(self):
        try:
            self._config
        except AttributeError:
            if self.segment_type == 0:
                self._config = VideoConfig
            else:
                self._config = ChannelConfig
        return self._config

    @property
    def es_manager(self):
        if self.segment_type == 0:
            sections = self.SECTIONS + (Sections.CHANNEL,)
            es_manager = VideoManager(sections=sections, upsert_sections=(Sections.SEGMENTS,))
        else:
            es_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        return es_manager

    @property
    def data_type(self):
        """ Maps segment integer type (0 = video, 1 = channel) to string"""
        data_type = self.segment_id_to_type[self.segment_type]
        return data_type

    @property
    def audit_utils(self):
        try:
            self._audit_utils
        except AttributeError:
            self._audit_utils = SegmentAuditUtils(self.segment_type)
        return self._audit_utils

    @property
    def s3(self):
        try:
            self._s3
        except AttributeError:
            self._s3 = SegmentExporter(self, bucket_name=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        return self._s3

    # pylint: disable=signature-differs
    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        self.remove_all_from_segment()
        super().delete(*args, **kwargs)
        return self
    # pylint: enable=signature-differs

    @staticmethod
    def get_featured_image_s3_key(uuid, extension):
        return CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY.format(
            uuid=uuid,
            extension=extension
        )

    def get_s3_key(self, *args, **kwargs):
        """
        get existing s3_key from related CustomSegmentFileUpload's
        filename field or make new key
        """
        if hasattr(self, 'export') and self.export.filename:
            return self.export.filename
        return f"{self.uuid}_export.csv"

    def get_vetted_s3_key(self, suffix=None):
        """
        get existing s3_key from related CustomSegmentVettedFileUpload's
        filename field or make new key
        """
        if hasattr(self, 'vetted_export') and self.vetted_export.filename and not suffix:
            return self.vetted_export.filename
        suffix = f"_{suffix}" if suffix is not None else ""
        return f"{self.uuid}_vetted_export{suffix}.csv"

    def get_source_s3_key(self):
        """
        get existing s3_key from related CustomSegmentSourceFileUpload's
        filename field or make new key
        """
        if hasattr(self, 'source') and self.source.filename:
            return self.source.filename
        return f"{self.uuid}_export_source.csv"

    def get_vetted_items_query(self):
        """
        Create query for processed vetted items
        :return:
        """
        annotation = {
            "yt_id": models.F(f"{self.config.DATA_FIELD}__{self.config.DATA_FIELD}_id")
        }
        audit = AuditProcessor.objects.get(id=self.audit_id)
        vetting_yt_ids = self.audit_utils.vetting_model.objects \
            .filter(audit=audit, processed__isnull=False) \
            .annotate(**annotation) \
            .values_list("yt_id", flat=True)
        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(list(vetting_yt_ids)).get()
        return query


class CustomSegmentRelated(models.Model):
    related_id = models.CharField(max_length=100)
    segment = models.ForeignKey(CustomSegment, related_name="related", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("segment", "related_id"),)
