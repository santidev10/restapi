"""
PersistentSegmentVideo models module
"""
from django.db.models import BigIntegerField
from django.db.models import Count
from django.db.models import ForeignKey
from django.db.models import Sum
from django.db.models.functions import Cast

from django.contrib.postgres.fields.jsonb import KeyTextTransform

from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from .constants import PersistentSegmentCategory


class PersistentSegmentVideo(BasePersistentSegment):
    segment_type = PersistentSegmentType.VIDEO

    objects = PersistentSegmentManager()

    def calculate_details(self):
        details = self.related.annotate(
            related_likes=Cast(KeyTextTransform("likes", "details"), BigIntegerField()),
            related_dislikes=Cast(KeyTextTransform("dislikes", "details"), BigIntegerField()),
            related_views=Cast(KeyTextTransform("views", "details"), BigIntegerField()),
        ).aggregate(
            likes=Sum("related_likes"),
            dislikes=Sum("related_dislikes"),
            views=Sum("related_views"),
            items_count=Count("id")
        )
        return details


class PersistentSegmentRelatedVideo(BasePersistentSegmentRelated):
    segment = ForeignKey(PersistentSegmentVideo, related_name="related")

    def get_url(self):
        return "https://www.youtube.com/video/{}".format(self.related_id)

    def get_exportable_row(self):
        details = self.details or {}
        row = {
            PersistentSegmentExportColumn.URL: self.get_url(),
            PersistentSegmentExportColumn.TITLE: self.title,
            PersistentSegmentExportColumn.CATEGORY: self.category,
            PersistentSegmentExportColumn.LANGUAGE: details.get("language"),
            PersistentSegmentExportColumn.THUMBNAIL: self.thumbnail_image_url,
            PersistentSegmentExportColumn.LIKES: details.get("likes"),
            PersistentSegmentExportColumn.DISLIKES: details.get("dislikes"),
            PersistentSegmentExportColumn.VIEWS: details.get("views"),
            PersistentSegmentExportColumn.BAD_WORDS: ",".join(details.get("bad_words", [])),
        }

        if self.segment.category == PersistentSegmentCategory.CUSTOM:
            row[PersistentSegmentExportColumn.CHANNEL_ID] = details.get('channel_id')
            row[PersistentSegmentExportColumn.CHANNEL_TITLE] = details.get('channel_title')

        return row
