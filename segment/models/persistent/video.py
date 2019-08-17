"""
PersistentSegmentVideo models module
"""
from django.db.models import BigIntegerField
from django.db.models import Count
from django.db.models import ForeignKey
from django.db.models import Sum
from django.db.models.functions import Cast
from django.contrib.postgres.fields.jsonb import KeyTextTransform

from audit_tool.models import AuditCategory
from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from .constants import PersistentSegmentCategory
from segment.api.serializers import PersistentSegmentVideoExportSerializer
from es_components.managers import VideoManager
from es_components.constants import Sections
from utils.es_components_api_utils import ESQuerysetAdapter


class PersistentSegmentVideo(BasePersistentSegment):
    segment_type = PersistentSegmentType.VIDEO
    export_serializer = PersistentSegmentVideoExportSerializer
    audit_category = ForeignKey(AuditCategory, related_name="video_segment", null=True)
    objects = PersistentSegmentManager()
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

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

    def get_queryset(self):
        queryset = ESQuerysetAdapter(VideoManager(sections=self.SECTIONS))
        queryset.filter(self.get_filter_query())
        queryset.order_by("views:dsc")
        return queryset

    def get_export_columns(self):
        if self.category == "whitelist":
            export_columns = PersistentSegmentExportColumn.VIDEO_WHITELIST_CSV_COLUMNS
        else:
            export_columns = PersistentSegmentExportColumn.VIDEO_BLACKLIST_CSV_COLUMNS
        return export_columns

    @staticmethod
    def get_title(category_name, list_type):
        return f"Videos {category_name} Brand Suitability {list_type.capitalize()}"


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
            PersistentSegmentExportColumn.OVERALL_SCORE: details.get("overall_score"),
            PersistentSegmentExportColumn.BAD_WORDS: ",".join(details.get("bad_words", [])),
        }

        if self.segment.category == PersistentSegmentCategory.TOPIC:
            row[PersistentSegmentExportColumn.CHANNEL_ID] = details.get('channel_id')
            row[PersistentSegmentExportColumn.CHANNEL_TITLE] = details.get('channel_title')

        return row
