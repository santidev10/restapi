"""
PersistentSegmentChannel models module
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
from ...names import PERSISTENT_SEGMENT_CHANNEL_CSV_COLUMNS
from ...names import PersistentSegmentExportColumn


class PersistentSegmentChannel(BasePersistentSegment):
    segment_type = PersistentSegmentType.CHANNEL
    export_columns = PERSISTENT_SEGMENT_CHANNEL_CSV_COLUMNS

    objects = PersistentSegmentManager()

    def calculate_details(self):
        details = self.related.annotate(
            related_subscribers=Cast(KeyTextTransform("subscribers", "details"), BigIntegerField()),
            related_likes=Cast(KeyTextTransform("likes", "details"), BigIntegerField()),
            related_dislikes=Cast(KeyTextTransform("dislikes", "details"), BigIntegerField()),
            related_views=Cast(KeyTextTransform("views", "details"), BigIntegerField()),
            related_audited_videos=Cast(KeyTextTransform("audited_videos", "details"), BigIntegerField()),
        ).aggregate(
            subscribers=Sum("related_subscribers"),
            likes=Sum("related_likes"),
            dislikes=Sum("related_dislikes"),
            views=Sum("related_views"),
            audited_videos=Sum("related_audited_videos"),
            items_count=Count("id")
        )
        return details


class PersistentSegmentRelatedChannel(BasePersistentSegmentRelated):
    segment = ForeignKey(PersistentSegmentChannel, related_name="related")

    def get_url(self):
        return "https://www.youtube.com/channel/{}".format(self.related_id)

    def get_exportable_row(self):
        details = self.details or {}
        row = {
            PersistentSegmentExportColumn.URL: self.get_url(),
            PersistentSegmentExportColumn.TITLE: self.title,
            PersistentSegmentExportColumn.CATEGORY: self.category,
            PersistentSegmentExportColumn.LANGUAGE: details.get("language"),
            PersistentSegmentExportColumn.THUMBNAIL: self.thumbnail_image_url,
            PersistentSegmentExportColumn.SUBSCRIBERS: details.get("subscribers"),
            PersistentSegmentExportColumn.LIKES: details.get("likes"),
            PersistentSegmentExportColumn.DISLIKES: details.get("dislikes"),
            PersistentSegmentExportColumn.VIEWS: details.get("views"),
            PersistentSegmentExportColumn.AUDITED_VIDEOS: details.get("audited_videos"),
            PersistentSegmentExportColumn.BAD_WORDS: ",".join(details.get("bad_words", [])),
        }
        return row
