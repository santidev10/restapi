"""
BaseSegment models module
"""
import logging
from uuid import uuid4

from django.conf import settings
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
from segment.api.export_serializers import CustomSegmentVideoExportSerializer
from segment.api.export_serializers import AdminCustomSegmentVideoExportSerializer
from segment.api.export_serializers import AdminCustomSegmentChannelExportSerializer
from segment.api.export_serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.export_serializers import CustomSegmentVideoVettedExportSerializer
from segment.models.constants import CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY
from segment.models.constants import ChannelConfig
from segment.models.constants import Params
from segment.models.constants import Results
from segment.models.constants import VideoConfig
from segment.models.constants import SegmentTypeEnum
from segment.models.segment_mixin import SegmentMixin
from segment.models.utils.segment_audit_utils import SegmentAuditUtils
from segment.models.utils.segment_exporter import SegmentExporter
from utils.models import Timestampable
from utils.datetime import now_in_default_tz

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
        (SegmentTypeEnum.VIDEO.value, VIDEO),
        (SegmentTypeEnum.CHANNEL.value, CHANNEL)
    )
    # audit_id is AuditProcessor id used for ctl vetting
    audit_id = models.IntegerField(null=True, default=None, db_index=True)
    uuid = models.UUIDField(unique=True, default=uuid4)
    # Store general statistics / results data. This should never be reset, always updated
    statistics = models.JSONField(default=dict)
    list_type = models.IntegerField(choices=LIST_TYPE_CHOICES, null=True, default=None)
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.CASCADE)
    segment_type = models.IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = models.CharField(max_length=255, db_index=True)
    title_hash = models.BigIntegerField(default=0, db_index=True)
    is_vetting_complete = models.BooleanField(default=False, db_index=True)
    is_featured = models.BooleanField(default=False, db_index=True)
    is_regenerating = models.BooleanField(default=False, db_index=True)
    featured_image_url = models.TextField(default="")
    # Store general ctl generation params. This should never be reset, always updated
    params = models.JSONField(default=dict)

    def remove_meta_audit_params(self):
        remove_keys = {
            Params.META_AUDIT_ID, Params.INCLUSION_FILE, Params.EXCLUSION_FILE,
        }
        [self.params.pop(key, None) for key in remove_keys]
        self.save(update_fields=["params"])

    @property
    def type(self):
        return SegmentTypeEnum(self.segment_type).name.lower()

    @property
    def user_export_serializer(self):
        """ Get user export serializer depending on if channel or video segment """
        if self.segment_type in (SegmentTypeEnum.VIDEO.value, "video"):
            serializer = CustomSegmentVideoExportSerializer
        else:
            serializer = CustomSegmentChannelExportSerializer
        return serializer

    @property
    def admin_export_serializer(self):
        """ Returns export serializer available only to admin """
        if self.segment_type in (SegmentTypeEnum.VIDEO.value, "video"):
            serializer = self._get_video_admin_serializer()
        else:
            serializer = self._get_channel_admin_serializer()
        return serializer

    def _get_video_admin_serializer(self):
        """ Get video admin export serializer depending on vetting status """
        if self.is_vetting is True:
            serializer = CustomSegmentVideoVettedExportSerializer
        else:
            serializer = AdminCustomSegmentVideoExportSerializer
        return serializer

    def _get_channel_admin_serializer(self):
        """ Get channel admin export serializer depending on vetting status """
        if self.is_vetting is True:
            serializer = CustomSegmentChannelVettedExportSerializer
        else:
            serializer = AdminCustomSegmentChannelExportSerializer
        return serializer

    @property
    def config(self):
        try:
            self._config
        except AttributeError:
            if self.segment_type == SegmentTypeEnum.VIDEO.value:
                self._config = VideoConfig
            else:
                self._config = ChannelConfig
        return self._config

    @property
    def es_manager(self):
        if self.segment_type == SegmentTypeEnum.VIDEO.value:
            sections = self.SECTIONS + (Sections.CHANNEL,)
            es_manager = VideoManager(sections=sections, upsert_sections=(Sections.SEGMENTS,))
        else:
            sections = self.SECTIONS + (Sections.IAS_DATA,)
            es_manager = ChannelManager(sections=sections, upsert_sections=(Sections.SEGMENTS,))
        return es_manager

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

    def get_admin_s3_key(self, *args, **kwargs):
        """
        get existing admin_s3_key from related CustomSegmentFileUpload's
        admin_filename field or make new key
        """
        if hasattr(self, 'export') and self.export.admin_filename:
            return self.export.admin_filename
        return f"{self.uuid}_admin_export.csv"

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

    def delete_related(self, *_, **__):
        """ Delete CTL and related objects in case of exceptions while creating ctl """
        def _delete_audit(audit_id):
            try:
                AuditProcessor.objects.get(id=audit_id).delete()
            except AuditProcessor.DoesNotExist:
                pass
        _delete_audit(self.audit_id)
        _delete_audit(self.params.get(Params.META_AUDIT_ID))
        self.delete()

    def update_statistics(self, nested_key, data, data_field, save=False):
        nested_data = self.statistics.get(nested_key)
        nested_data[data_field] = data
        self.statistics[nested_key] = nested_data
        if save is True:
            self.save(update_fields=["statistics"])

    def update_params(self, data, nested_key: str, data_field=None, save=False) -> None:
        """
        Method to handle updating nested data in params JSON field

        :param data:
        :param nested_key: Key for storing nested data in params
        :param data_field: Optional key to set data on nested data. If data_field is None, then it is implied that
            data is a dict and will be used to update the nested data dict

            self.params = {
                "gads_sync_data": {...}
                "dv360_sync_data": {...},
            }
            nested_key = "gads_sync_data"
            self.params[nested_key].update(data)

        :usage:
            self.params = {
                "gads_sync_data": {...}
                "dv360_sync_data": {...},
            }
            nested_key = "gads_sync_data"
            data_field = "adgroup_ids"
            self.params[nested_key][data_field] = data
        :param save: If True, then save instance
        """
        nested_data = self.params.get(nested_key)
        if data_field is not None:
            nested_data[data_field] = data
        else:
            nested_data.update(data)
        self.params[nested_key] = nested_data
        if save is True:
            self.save(update_fields=["params"])

    def update_sync_history(self, account_name, sync_type) -> None:
        """
        Add date to ctl sync history
        :param account_name: str -> Name of external resource entity e.g. GAds cid Account name
        :param sync_type: str -> Name of external resource e.g. gads or dv360
        """
        date_str = now_in_default_tz().strftime("%H:%M, %B %d, %Y")
        message = f"{account_name} - at {date_str}"
        self.statistics[sync_type] = self.statistics.get(sync_type, {})
        prev_history = self.statistics[sync_type].get(Results.HISTORY, [])
        self.statistics[sync_type][Results.HISTORY] = [*prev_history, message]


class CustomSegmentRelated(models.Model):
    related_id = models.CharField(max_length=100)
    segment = models.ForeignKey(CustomSegment, related_name="related", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("segment", "related_id"),)
