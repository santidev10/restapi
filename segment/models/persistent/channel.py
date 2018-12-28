"""
PersistentSegmentChannel models module
"""
from django.db.models import ForeignKey

from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType


class PersistentSegmentChannel(BasePersistentSegment):
    segment_type = PersistentSegmentType.CHANNEL

    objects = PersistentSegmentManager()


class PersistentSegmentRelatedChannel(BasePersistentSegmentRelated):
    segment = ForeignKey(PersistentSegmentChannel, related_name="related")
