"""
BaseSegment models module
"""
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import CASCADE
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
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics
from segment.utils import retry_on_conflict
from utils.models import Timestampable


logger = logging.getLogger(__name__)


class CustomSegment(Timestampable):
    """
    Base segment model
    """
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.segment_type == 0:
            self.related_aw_statistics_model = YTVideoStatistic
        else:
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
    statistics = JSONField(default=dict())
    list_type = IntegerField(choices=LIST_TYPE_CHOICES)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=CASCADE)
    segment_type = IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = CharField(max_length=255, db_index=True)
    title_hash = BigIntegerField(default=0, db_index=True)

    def delete(self, *args, **kwargs):
        # Delete segment references from Elasticsearch
        retry_on_conflict(self.remove_all_from_segment, retry_amount=self.REMOVE_FROM_SEGMENT_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFF)
        super().delete(*args, **kwargs)
        return self

    def calculate_statistics(self, items_count):
        """
        Aggregate statistics
        :param items_count: int
        :return:
        """
        es_manager = self.get_es_manager(sections=(Sections.GENERAL_DATA,))
        query = self.get_segment_items_query()
        result = es_manager.search(query, limit=settings.MAX_SEGMENT_TO_AGGREGATE).execute()

        top_three_items = []
        all_ids = []
        for doc in result.hits:
            all_ids.append(doc.main.id)
            # Check if we data to display for each item in top three
            if len(top_three_items) < 3 and getattr(doc.general_data, "title", None) and getattr(doc.general_data, "thumbnail_image_url", None):
                top_three_items.append({
                    "id": doc.main.id,
                    "title": doc.general_data.title,
                    "image_url": doc.general_data.thumbnail_image_url
                })

        statistics = {
            "adw_data": aggregate_segment_statistics(self, all_ids),
            "items_count": items_count,
            "top_three_items": top_three_items
        }
        return statistics

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

    def remove_all_from_segment(self):
        """
        Remove all references to segment uuid from Elasticsearch
        :return:
        """
        es_manager = self.get_es_manager()
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        es_manager.remove_from_segment(query, self.uuid)

    def get_segment_items_query(self):
        """
        Get query to get segment documents
        :return:
        """
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        return query

    def get_serializer(self):
        """
        Get export serializer
        :return:
        """
        if self.segment_type == 0:
            return CustomSegmentVideoExportSerializer
        else:
            return CustomSegmentChannelExportSerializer


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related", on_delete=CASCADE)

    class Meta:
        unique_together = (('segment', 'related_id'),)
