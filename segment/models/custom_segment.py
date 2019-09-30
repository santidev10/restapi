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

from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST
from es_components.constants import Sections
from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.constants import VIEWS_FIELD
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import SortDirections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer
from segment.models.utils.calculate_segment_statistics import calculate_statistics
from segment.models.segment_mixin import SegmentUtils
from segment.models.utils.export_context_manager import ExportContextManager
from segment.utils import generate_search_with_params
from segment.utils import retry_on_conflict
from segment.models.segment_mixin import SegmentMixin
from utils.models import Timestampable


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
        self.segment_utils = SegmentUtils(self)

        if self.segment_type == 0:
            self.SORT_KEY = {VIEWS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 20000
            self.related_aw_statistics_model = YTVideoStatistic
        else:
            self.SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
            self.LIST_SIZE = 20000
            self.related_aw_statistics_model = YTChannelStatistic

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

    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        self.remove_all_from_segment()
        super().delete(*args, **kwargs)
        return self

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
            return CustomSegmentVideoExportSerializer
        else:
            return CustomSegmentChannelExportSerializer

    def get_s3_key(self):
        return f"{self.owner_id}/{self.title}.csv"


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related", on_delete=CASCADE)

    class Meta:
        unique_together = (('segment', 'related_id'),)
