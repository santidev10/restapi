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
from .constants import PersistentSegmentExportColumn
from es_components.managers import ChannelManager
from es_components.constants import Sections
from utils.es_components_api_utils import ESQuerysetAdapter
from segment.api.serializers import PersistentSegmentChannelExportSerializer


class PersistentSegmentChannel(BasePersistentSegment):
    segment_type = PersistentSegmentType.CHANNEL
    export_serializer = PersistentSegmentChannelExportSerializer
    objects = PersistentSegmentManager()
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

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

    def get_queryset(self):
        queryset = ESQuerysetAdapter(ChannelManager(sections=self.SECTIONS))
        queryset.order_by("stats.subscribers:desc")
        queryset.filter([self.get_filter_query()])
        return queryset

    def get_export_columns(self):
        if self.category == "whitelist":
            export_columns = PersistentSegmentExportColumn.CHANNEL_WHITELIST_CSV_COLUMNS
        else:
            export_columns = PersistentSegmentExportColumn.CHANNEL_BLACKLIST_CSV_COLUMNS
        return export_columns

    @staticmethod
    def get_title(category_name, list_type):
        return f"Channels {category_name} Brand Suitability {list_type.capitalize()}"


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
            PersistentSegmentExportColumn.OVERALL_SCORE: details.get("overall_score"),
            PersistentSegmentExportColumn.BAD_WORDS: ",".join(details.get("bad_words", [])),
        }
        return row


