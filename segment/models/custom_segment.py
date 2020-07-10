"""
BaseSegment models module
"""
import logging
from uuid import uuid4

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import BooleanField
from django.db.models import CASCADE
from django.db.models import CharField
from django.db.models import F
from django.db.models import ForeignKey
from django.db.models import IntegerField
from django.db.models import Model
from django.db.models import TextField
from django.db.models import UUIDField
from django.utils import timezone

from audit_tool.models import AuditProcessor
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import VIEWS_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers.custom_segment_export_serializers import \
    CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer
from segment.models.constants import CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY
from segment.models.persistent.constants import CHANNEL_SOURCE_FIELDS
from segment.models.persistent.constants import VIDEO_SOURCE_FIELDS
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
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1
    SORT_KEY = None
    LIST_SIZE = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_exporter = SegmentExporter(bucket_name=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        self.audit_utils = SegmentAuditUtils(self.segment_type)
        self._serializer = None

        if self.segment_type == 0:
            self.data_field = "video"
            # AuditProcessor audit_type
            self.audit_type = 1
            self.SORT_KEY = {VIEWS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 100000
            self.SOURCE_FIELDS = VIDEO_SOURCE_FIELDS
            self.related_aw_statistics_model = YTVideoStatistic
            self.es_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        else:
            self.data_field = "channel"
            self.audit_type = 2
            self.SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 100000
            self.SOURCE_FIELDS = CHANNEL_SOURCE_FIELDS
            self.related_aw_statistics_model = YTChannelStatistic
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
    segment_id_to_type = {
        _id: segment_type for segment_type, _id in segment_type_to_id.items()
    }
    list_id_to_type = {
        _id: list_type for list_type, _id in list_type_to_id.items()
    }

    audit_id = IntegerField(null=True, default=None, db_index=True)
    uuid = UUIDField(unique=True, default=uuid4)
    statistics = JSONField(default=dict)
    list_type = IntegerField(choices=LIST_TYPE_CHOICES, null=True, default=None)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=CASCADE)
    segment_type = IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = CharField(max_length=255, db_index=True)
    title_hash = BigIntegerField(default=0, db_index=True)
    is_vetting_complete = BooleanField(default=False, db_index=True)
    is_featured = BooleanField(default=False, db_index=True)
    is_regenerating = BooleanField(default=False, db_index=True)
    featured_image_url = TextField(default="")

    @property
    def data_type(self):
        data_type = self.segment_id_to_type[self.segment_type]
        return data_type

    @property
    def serializer(self):
        if self._serializer:
            return self._serializer

        if self.segment_type == 0:
            self._serializer = CustomSegmentVideoExportSerializer
        elif self.owner and self.owner.has_perm("userprofile.monetization_filter"):
            self._serializer = CustomSegmentChannelWithMonetizationExportSerializer
            self.SOURCE_FIELDS += (f"{Sections.MONETIZATION}.is_monetizable",)
        else:
            self._serializer = CustomSegmentChannelExportSerializer
        return self._serializer

    @serializer.setter
    def serializer(self, serializer):
        self._serializer = serializer

    def set_es_sections(self, sections, upsert_sections):
        self.es_manager.sections = sections
        self.es_manager.upsert_sections = upsert_sections

    # pylint: disable=signature-differs
    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        self.remove_all_from_segment()
        super().delete(*args, **kwargs)
        return self
    # pylint: enable=signature-differs

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
            s3_key = self.export.parse_download_url()
        export_content = self.s3_exporter.get_s3_export_content(s3_key, get_key=False).iter_chunks()
        return export_content

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

    def delete_export(self, s3_key=None):
        """
        Delete csv from s3
        :param s3_key: str -> S3 file keyname
        :return:
        """
        if s3_key is None:
            s3_key = self.get_s3_key()
        self.s3_exporter.delete_obj(s3_key)

    def get_extract_export_ids(self, s3_key=None):
        """
        Parse and extract Channel or video ids from csv export
        :return:
        """
        if s3_key is None:
            s3_key = self.get_s3_key()
        # pylint: disable=protected-access
        export_content = self.s3_exporter._get_s3_object(s3_key, get_key=False)
        # pylint: enable=protected-access
        url_index = None
        for byte in export_content["Body"].iter_lines():
            row = (byte.decode("utf-8")).split(",")
            if url_index is None:
                try:
                    url_index = row.index("URL")
                    continue
                except ValueError:
                    url_index = 0
            item_id = self.parse_url(row[url_index], self.segment_type)
            yield item_id

    def parse_url(self, url, item_type="0"):
        item_type = str(item_type)
        config = {
            "0": "/watch?v=",
            "1": "/channel/",
        }
        item_id = url.split(config[item_type])[-1]
        return item_id

    def get_vetted_items_query(self):
        """
        Create query for processed vetted items
        :return:
        """
        annotation = {
            "yt_id": F(f"{self.data_field}__{self.data_field}_id")
        }
        audit = AuditProcessor.objects.get(id=self.audit_id)
        vetting_yt_ids = self.audit_utils.vetting_model.objects \
            .filter(audit=audit, processed__isnull=False) \
            .annotate(**annotation) \
            .values_list("yt_id", flat=True)
        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(list(vetting_yt_ids)).get()
        return query


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related", on_delete=CASCADE)

    class Meta:
        unique_together = (("segment", "related_id"),)
