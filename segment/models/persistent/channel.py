"""
PersistentSegmentChannel models module
"""
from django.db.models import ForeignKey
from django.db.models import CASCADE

from audit_tool.models import AuditCategory
from aw_reporting.models import YTChannelStatistic
from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from es_components.managers import ChannelManager
from es_components.constants import Sections
from segment.api.serializers.persistent_segment_export_serializer import PersistentSegmentChannelExportSerializer
from segment.utils import generate_search_with_params


class PersistentSegmentChannel(BasePersistentSegment):
    segment_type = PersistentSegmentType.CHANNEL
    export_serializer = PersistentSegmentChannelExportSerializer
    objects = PersistentSegmentManager()
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    related_aw_statistics_model = YTChannelStatistic

    def get_es_manager(self, sections=None):
        if sections is None:
            sections = self.SECTIONS
        es_manager = ChannelManager(sections=sections)
        return es_manager

    def get_queryset(self, sections=None):
        if sections is None:
            sections = self.SECTIONS
        sort_key = {"stats.subscribers": {"order": "desc"}}
        es_manager = self.get_es_manager(sections=sections)
        scan = generate_search_with_params(es_manager, self.get_segment_items_query(), sort_key).scan()
        return scan

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
    segment = ForeignKey(PersistentSegmentChannel, related_name="related", on_delete=CASCADE)

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


