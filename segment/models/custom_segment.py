"""
BaseSegment models module
"""
import collections.abc
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import CASCADE
from django.db.models import CharField
from django.db.models import IntegerField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import UUIDField

from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST
from es_components.constants import Sections
from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.api.serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers import CustomSegmentVideoExportSerializer
from segment.models.utils.custom_segment_channel_statistics import CustomSegmentChannelStatistics
from segment.models.utils.custom_segment_video_statistics import CustomSegmentVideoStatistics
from utils.models import Timestampable

logger = logging.getLogger(__name__)

MAX_ITEMS_GET_FROM_SINGLEDB = 10000
MAX_ITEMS_DELETE_FROM_DB = 10


class CustomSegment(Timestampable):
    """
    Base segment model
    """
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.segment_type == 0:
            self.stats_util = CustomSegmentVideoStatistics()
        else:
            self.stats_util = CustomSegmentChannelStatistics()
        self.related_aw_statistics_model = self.stats_util.related_aw_statistics_model

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

    @property
    def es_manager(self):
        if self.segment_type == 0:
            return VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        else:
            return ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

    @property
    def related_ids(self):
        return self.related.values_list("related_id", flat=True)

    def add_related_ids(self, ids):
        if not isinstance(ids, collections.abc.Sequence) and isinstance(ids, str):
            ids = [ids]
        to_create = set(ids) - set(self.related_ids)
        CustomSegmentRelated.objects.bulk_create([CustomSegmentRelated(segment_id=self.id, related_id=_id) for _id in to_create])

    def update_statistics(self):
        """
        Process segment statistics fields
        """
        end = None if self.related_ids.count() < settings.MAX_SEGMENT_TO_AGGREGATE else settings.MAX_SEGMENT_TO_AGGREGATE
        data = self.stats_util.obtain_singledb_data(self.related_ids, end=end)
        updated_statistics = self.stats_util.get_statistics(self, data)
        self.statistics.update(updated_statistics)
        self.save()
        return "Done"

    def get_es_manager(self):
        if self.segment_type == 0:
            return VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        else:
            return ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

    def remove_all_from_segment(self):
        query = QueryBuilder.build().must().term().field(Sections.SEGMENTS).value(self.uuid).get()
        self.es_manager.remove_from_segment(query, self.uuid)

    def get_segment_items_query(self):
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        return query

    def get_serializer(self):
        if self.segment_type == 0:
            return CustomSegmentVideoExportSerializer
        else:
            return CustomSegmentChannelExportSerializer


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related")

    class Meta:
        unique_together = (('segment', 'related_id'),)
