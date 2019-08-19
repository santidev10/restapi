"""
PersistentSegmentChannel models module
"""
from django.db.models import ForeignKey

from audit_tool.models import AuditCategory
from .base import BasePersistentSegment
from .base import BasePersistentSegmentRelated
from .base import PersistentSegmentManager
from .constants import PersistentSegmentType
from .constants import PersistentSegmentExportColumn
from es_components.managers import ChannelManager
from es_components.constants import Sections
from utils.es_components_api_utils import ESQuerysetAdapter
from segment.api.serializers.persistent_segment_export_serializer import PersistentSegmentChannelExportSerializer


class PersistentSegmentChannel(BasePersistentSegment):
    segment_type = PersistentSegmentType.CHANNEL
    export_serializer = PersistentSegmentChannelExportSerializer
    audit_category = ForeignKey(AuditCategory, related_name="channel_segment", null=True)
    objects = PersistentSegmentManager()
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

    def calculate_details(self):
        es_manager = ChannelManager(sections=self.SECTIONS)
        search = es_manager.search(query=self.get_segment_items_query())
        search.aggs.bucket("subscribers", "sum", field=f"{Sections.STATS}.subscribers")
        search.aggs.bucket("likes",  "sum", field=f"{Sections.STATS}.likes")
        search.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.dislikes")
        search.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")
        search.aggs.bucket("audited_videos", "sum", field=f"{Sections.BRAND_SAFETY}.videos_scored")
        result = search.execute()
        details = self.extract_aggregations(result.aggregations.to_dict())
        details["items_count"] = result.hits.total
        return details

    def get_queryset(self):
        queryset = ESQuerysetAdapter(ChannelManager(sections=self.SECTIONS))
        queryset.order_by("stats.subscribers:desc")
        queryset.filter([self.get_segment_items_query()])
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


