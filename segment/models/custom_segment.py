"""
BaseSegment models module
"""
import collections.abc
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import CharField
from django.db.models import IntegerField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import SET_NULL

from utils.models import Timestampable

from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST

from segment.models.utils.custom_segment_channel_statistics import CustomSegmentChannelStatistics
from segment.models.utils.custom_segment_video_statistics import CustomSegmentVideoStatistics

logger = logging.getLogger(__name__)

MAX_ITEMS_GET_FROM_SINGLEDB = 10000
MAX_ITEMS_DELETE_FROM_DB = 10


class CustomSegment(Timestampable):
    """
    Base segment model
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats_util = CustomSegmentVideoStatistics() if self.segment_type == 0 else CustomSegmentChannelStatistics()
        self.related_aw_statistics_model = self.stats_util.related_aw_statistics_model

    LIST_TYPE_CHOICES = (
        (0, WHITELIST),
        (1, BLACKLIST)
    )
    SEGMENT_TYPE_CHOICES = (
        (0, VIDEO),
        (1, CHANNEL)
    )
    statistics = JSONField(default=dict())
    list_type = IntegerField(choices=LIST_TYPE_CHOICES)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=SET_NULL)
    segment_type = IntegerField(choices=SEGMENT_TYPE_CHOICES)
    title = CharField(max_length=255)
    title_hash = BigIntegerField(default=0, db_index=True)

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


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related")

    class Meta:
        unique_together = (('segment', 'related_id'),)
