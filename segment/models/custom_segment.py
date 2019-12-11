"""
BaseSegment models module
"""
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import CharField
from django.db.models import IntegerField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import CASCADE
from django.db.models import UUIDField
from django.utils import timezone

from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST
from es_components.constants import Sections
from es_components.constants import VIEWS_FIELD
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import SortDirections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer
from segment.models.segment_mixin import SegmentMixin
from segment.models.persistent.constants import CHANNEL_SOURCE_FIELDS
from segment.models.persistent.constants import VIDEO_SOURCE_FIELDS
from utils.models import Timestampable
from segment.models.utils.segment_exporter import SegmentExporter

logger = logging.getLogger(__name__)


class CustomSegment(SegmentMixin, Timestampable):
    """
    Base segment model
    """
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1
    SORT_KEY = None
    LIST_SIZE = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_exporter = SegmentExporter(bucket_name=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)

        if self.segment_type == 0:
            self.SORT_KEY = {VIEWS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 20000
            self.SOURCE_FIELDS = VIDEO_SOURCE_FIELDS
            self.related_aw_statistics_model = YTVideoStatistic
            self.serializer = CustomSegmentVideoExportSerializer
            self.es_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        else:
            self.SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 20000
            self.SOURCE_FIELDS = CHANNEL_SOURCE_FIELDS
            self.related_aw_statistics_model = YTChannelStatistic
            self.serializer = CustomSegmentChannelExportSerializer
            self.es_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

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

    uuid = UUIDField(unique=True)
    statistics = JSONField(default=dict)
    list_type = IntegerField(choices=LIST_TYPE_CHOICES)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=CASCADE)
    segment_type = IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = CharField(max_length=255, db_index=True)
    title_hash = BigIntegerField(default=0, db_index=True)

    def set_es_sections(self, sections, upsert_sections):
        self.es_manager.sections = sections
        self.es_manager.upsert_sections = upsert_sections

    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        self.remove_all_from_segment()
        super().delete(*args, **kwargs)
        return self

    def export_file(self, s3_key=None, updating=False, queryset=None):
        now = timezone.now()
        export = self.export
        if s3_key is None:
            s3_key = self.get_s3_key()
        if updating:
            export.updated_at = now
        else:
            export.completed_at = now
        self.s3_exporter.export_to_s3(self, s3_key, queryset=queryset)
        download_url = self.s3_exporter.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        export.download_url = download_url
        export.save()

    def get_export_file(self, s3_key=None):
        if s3_key is None:
            s3_key = self.get_s3_key()
        export_content = self.s3_exporter.get_s3_export_content(s3_key, get_key=False).iter_chunks()
        return export_content

    def get_es_manager(self, sections=None):
        """
        Get Elasticsearch manager based on segment type
        :param sections:
        :return:
        """
        if sections is None:
            sections = self.SECTIONS
        if self.segment_type == 0:
            return VideoManager(sections=sections, upsert_sections=(Sections.SEGMENTS,))
        else:
            return ChannelManager(sections=sections, upsert_sections=(Sections.SEGMENTS,))

    def get_serializer(self):
        """
        Get export serializer
        :return:
        """
        if self.segment_type == 0:
            return
        else:
            return CustomSegmentChannelExportSerializer

    def get_s3_key(self, *args, **kwargs):
        return f"custom_segments/{self.owner_id}/{self.title}.csv"

    def delete_export(self, s3_key=None):
        """
        Delete csv from s3
        :param s3_key: str -> S3 file keyname
        :return:
        """
        if s3_key is None:
            s3_key = self.get_s3_key()
        self.s3_exporter.delete_obj(s3_key)


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related", on_delete=CASCADE)

    class Meta:
        unique_together = (('segment', 'related_id'),)
