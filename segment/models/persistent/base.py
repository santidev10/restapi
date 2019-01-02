"""
BasePersistentSegment models module
"""
import logging

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField
from django.db.models import CharField
from django.db.models import TextField
from django.db.models import Manager

from utils.models import Timestampable

logger = logging.getLogger(__name__)


class PersistentSegmentManager(Manager):
    """
    Extend default persistent segment manager
    """


class BasePersistentSegment(Timestampable):
    """
    Base persistent segment model
    """
    title = CharField(max_length=255, null=True, blank=True)
    shared_with = ArrayField(CharField(max_length=200), blank=True, default=list)
    related = None  # abstract property
    segment_type = None  # abstract property

    class Meta:
        abstract = True
        ordering = ["pk"]

    @property
    def shared_with_string(self, separation_symbol="|"):
        return separation_symbol.join(self.shared_with)


class BasePersistentSegmentRelated(Timestampable):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=100)
    category = CharField(max_length=100, default="")
    title = TextField(default="")
    thumbnail_image_url = TextField(default="")

    details = JSONField(default=dict())

    class Meta:
        abstract = True
        unique_together = (
            ("segment", "related_id"),
        )
