"""
PersistentSegmentVideo models module
"""
from django.db.models import CharField
from django.db.models import ForeignKey

from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentCategory
from .constants import PersistentSegmentType


class PersistentSegmentVideo(BasePersistentSegment):
    CATEGORIES = PersistentSegmentCategory.ALL_OPTIONS
    segment_type = PersistentSegmentType.VIDEO

    category = CharField(max_length=255, choices=CATEGORIES)

    objects = PersistentSegmentManager()


class PersistentSegmentRelatedVideo(BasePersistentSegmentRelated):
    segment = ForeignKey(PersistentSegmentVideo, related_name="related")
