"""
BaseSegment models module
"""
import collections.abc
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db import IntegrityError
from django.db.models import CharField
from django.db.models import IntegerField
from django.db.models import ForeignKey
from django.db.models import Manager
from django.db.models import Model
from django.db.models import ManyToManyField
from django.db.models import SET_NULL

from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.models import Timestampable
from utils.utils import chunks_generator

from brand_safety.constants import BLACKLIST
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.constants import WHITELIST

logger = logging.getLogger(__name__)

MAX_ITEMS_GET_FROM_SINGLEDB = 10000
MAX_ITEMS_DELETE_FROM_DB = 10


class CustomSegment(Timestampable):
    """
    Base segment model
    """
    LIST_TYPE_CHOICES = (
        (0, WHITELIST),
        (1, BLACKLIST)
    )
    SEGMENT_TYPE_CHOICES = (
        (0, VIDEO),
        (1, CHANNEL)
    )
    list_type = IntegerField(choices=LIST_TYPE_CHOICES)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=SET_NULL)
    segment_type = IntegerField(choices=SEGMENT_TYPE_CHOICES)
    title = CharField(max_length=255, null=True, blank=True)

    def add_related_ids(self, ids):
        if not isinstance(ids, collections.abc.Sequence) and isinstance(ids, str):
            ids = [ids]
        existing = CustomSegmentRelated.objects.filter(related_id__in=ids)
        to_create = set(ids) - set(existing.values_list("related_id", flat=True))
        created = CustomSegmentRelated.objects.bulk_create([CustomSegmentRelated(segment_id=self.id, related_id=_id) for _id in to_create])
        self.related.add(*list(existing) + list(created))


class CustomSegmentRelated(Model):
    related_id = CharField(max_length=100)
    segment = ForeignKey(CustomSegment, related_name="related")

    class Meta:
        unique_together = (('segment', 'related_id'),)
