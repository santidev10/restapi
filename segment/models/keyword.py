"""
SegmentKeyword models module
"""
from celery import task
from django.db.models import CharField
from django.db.models import ForeignKey

from .base import BaseSegment
from .base import BaseSegmentRelated
from .base import SegmentManager


class SegmentKeyword(BaseSegment):
    CHF = "channel_factory"
    BLACKLIST = "blacklist"
    PRIVATE = "private"

    CATEGORIES = (
        (CHF, CHF),
        (BLACKLIST, BLACKLIST),
        (PRIVATE, PRIVATE),
    )

    category = CharField(max_length=255, choices=CATEGORIES)

    segment_type = 'keyword'

    objects = SegmentManager()

    @task
    def update_statistics(self):
        pass
 

class SegmentRelatedKeyword(BaseSegmentRelated):
    segment = ForeignKey(SegmentKeyword, related_name='related')
