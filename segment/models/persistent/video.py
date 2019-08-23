"""
PersistentSegmentVideo models module
"""
from django.db.models import ForeignKey
from django.db.models import CASCADE

from audit_tool.models import AuditCategory
from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from .constants import PersistentSegmentCategory
from es_components.managers import VideoManager
from es_components.constants import Sections
from segment.api.serializers import PersistentSegmentVideoExportSerializer
from segment.utils import generate_search_with_params


class PersistentSegmentVideo(BasePersistentSegment):
    segment_type = PersistentSegmentType.VIDEO
    export_serializer = PersistentSegmentVideoExportSerializer
    audit_category = ForeignKey(AuditCategory, related_name="video_segment", null=True, on_delete=CASCADE)
    objects = PersistentSegmentManager()
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)

    def calculate_details(self):
        es_manager = self.get_es_manager()
        search = es_manager.search(query=self.get_segment_items_query())
        search.aggs.bucket("likes",  "sum", field=f"{Sections.STATS}.likes")
        search.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.dislikes")
        search.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")
        result = search.execute()
        details = self.extract_aggregations(result.aggregations.to_dict())
        details["items_count"] = result.hits.total
        return details

    def get_es_manager(self, sections=None):
        if sections is None:
            sections = self.SECTIONS
        es_manager = VideoManager(sections=sections)
        return es_manager

    def get_queryset(self, sections=None):
        if sections is None:
            sections = self.SECTIONS
        sort_key = {"stats.views": {"order": "desc"}}
        es_manager = self.get_es_manager(sections=sections)
        scan = generate_search_with_params(es_manager, self.get_segment_items_query(), sort_key).scan()
        return scan

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
