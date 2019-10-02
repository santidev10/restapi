"""
PersistentSegmentVideo models module
"""
from django.db.models import ForeignKey
from django.db.models import CASCADE

from audit_tool.models import AuditCategory
from aw_reporting.models import YTVideoStatistic
from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from .constants import PersistentSegmentCategory
from es_components.managers import VideoManager
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import VIEWS_FIELD
from segment.api.serializers import PersistentSegmentVideoExportSerializer
from segment.models.segment_mixin import SegmentMixin


class PersistentSegmentVideo(SegmentMixin, BasePersistentSegment):
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    SORT_KEY = {VIEWS_FIELD: {"order": SortDirections.DESCENDING}}
    segment_type = PersistentSegmentType.VIDEO
    serializer = PersistentSegmentVideoExportSerializer
    objects = PersistentSegmentManager()
    related_aw_statistics_model = YTVideoStatistic

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

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
    segment = ForeignKey(PersistentSegmentVideo, related_name="related", on_delete=CASCADE)

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
