"""
PersistentSegmentVideo models module
"""
from django.db.models import ForeignKey

from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType


class PersistentSegmentVideo(BasePersistentSegment):
    segment_type = PersistentSegmentType.VIDEO

    objects = PersistentSegmentManager()


class PersistentSegmentRelatedVideo(BasePersistentSegmentRelated):
    segment = ForeignKey(PersistentSegmentVideo, related_name="related")

    def get_url(self):
        return "https://www.youtube.com/video/{}".format(self.related_id)
