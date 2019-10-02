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
from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from segment.api.serializers.persistent_segment_export_serializer import PersistentSegmentChannelExportSerializer
from segment.models.segment_mixin import SegmentMixin


class PersistentSegmentChannel(SegmentMixin, BasePersistentSegment):
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)
    SORT_KEY = {SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}
    segment_type = PersistentSegmentType.CHANNEL
    serializer = PersistentSegmentChannelExportSerializer
    objects = PersistentSegmentManager()
    related_aw_statistics_model = YTChannelStatistic

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

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


