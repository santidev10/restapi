import csv

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.models import BadWordCategory
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import Sections
from es_components.constants import VIEWS_FIELD
from es_components.query_builder import QueryBuilder
from segment.api.serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers import CustomSegmentVideoExportSerializer
from segment.api.serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.serializers import CustomSegmentVideoVettedExportSerializer
from segment.models.persistent.constants import YT_GENRE_CHANNELS
from utils.brand_safety import map_brand_safety_score


class GenerateSegmentUtils:
    _default_context = None
    _vetting = False
    segment = None

    def __init__(self, segment):
        self.segment_type = None
        self.segment = segment

    def set_vetting(self, vetting):
        """ Set vetting flag """
        self._vetting = vetting

    @staticmethod
    def get_vetting_data(segment, item_ids):
        """ Retrieve Postgres vetting data for serialization """
        try:
            vetting = AuditUtils.get_vetting_data(
                segment.audit_utils.vetting_model, segment.audit_id, item_ids, segment.DATA_FIELD
            )
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            vetting = {}
        return vetting

    @property
    def default_search_config(self):
        segment_type = self.segment.segment_type
        if segment_type in (0, "video"):
            config = self._get_default_video_search_config
        else:
            config = self._get_default_channel_search_config
        return config

    @property
    def default_serialization_context(self):
        """
        Get default serialization context for serializers
        Retrieve mappings from Postgres before serialization for efficiency
        """
        if self._default_context is not None:
            context = self._default_context
        else:
            brand_safety_categories = {
                category.id: category.name
                for category in BadWordCategory.objects.all()
            }
            self._default_context = context = {
                "brand_safety_categories": brand_safety_categories,
                "age_groups": AuditAgeGroup.to_str,
                "genders": AuditGender.to_str,
                "content_types": AuditContentType.to_str,
            }
        return context

    def _get_default_video_search_config(self):
        """ Get default video search config for bulk search function """
        config = dict(
            cursor_field=VIEWS_FIELD,
            # Exclude all age_restricted items
            options=[
                QueryBuilder().build().must().term().field("general_data.age_restricted").value(False).get()
            ]
        )
        return config

    def _get_default_channel_search_config(self):
        """ Get default channel search config for bulk search function """
        config = dict(
            cursor_field=SUBSCRIBERS_FIELD,
            # If channel, retrieve is_monetizable channels first then non-is_monetizable channels
            # for is_monetizable channel items to appear first on export
            options=[
                QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(
                    True).get(),
                QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(
                    True).get(),
            ]
        )
        return config

    def write_to_file(self, items, filename, segment, serializer_context, aggregations, write_header=False, mode="a"):
        """ Write data to csv file """
        rows = []
        fieldnames = segment.serializer.columns
        for item in items:
            # YT_GENRE_CHANNELS have no data and should not be on any export
            if item.main.id in YT_GENRE_CHANNELS:
                continue
            row = segment.serializer(item, context=serializer_context).data
            rows.append(row)
        with open(filename, mode=mode, newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if write_header is True:
                writer.writeheader()
            writer.writerows(rows)
        self.add_aggregations(aggregations, items, segment.segment_type)

    @staticmethod
    def add_aggregations(aggregations, items, segment_type):
        """
        Add values to aggregations
        Aggregations are performed in Python as calculating in Elasticsearch appears to overload memory usage
        """
        for item in items:
            # Calculating aggregations with each items already retrieved is much more efficient than
            # executing an additional aggregation query
            aggregations["monthly_views"] += item.stats.last_30day_views or 0
            aggregations["average_brand_safety_score"] += item.brand_safety.overall_score or 0
            aggregations["views"] += item.stats.views or 0
            aggregations["ctr"] += item.ads_stats.ctr or 0
            aggregations["ctr_v"] += item.ads_stats.ctr_v or 0
            aggregations["video_view_rate"] += item.ads_stats.video_view_rate or 0
            aggregations["average_cpm"] += item.ads_stats.average_cpm or 0
            aggregations["average_cpv"] += item.ads_stats.average_cpv or 0

            if segment_type in (0, "video"):
                aggregations["likes"] += item.stats.likes or 0
                aggregations["dislikes"] += item.stats.dislikes or 0
            else:
                aggregations["likes"] += item.stats.observed_videos_likes or 0
                aggregations["dislikes"] += item.stats.observed_videos_dislikes or 0
                aggregations["monthly_subscribers"] += item.stats.last_30day_subscribers or 0
                aggregations["subscribers"] += item.stats.subscribers or 0
                aggregations["audited_videos"] += item.brand_safety.videos_scored or 0

    @staticmethod
    def finalize_aggregations(aggregations, count):
        """ Finalize aggregations and calculate averages"""
        aggregations["average_brand_safety_score"] = map_brand_safety_score(
            aggregations["average_brand_safety_score"] // (count or 1))
        aggregations["ctr"] /= count or 1
        aggregations["ctr_v"] /= count or 1
        aggregations["video_view_rate"] /= count or 1
        aggregations["average_cpm"] /= count or 1
        aggregations["average_cpv"] /= count or 1
        return aggregations

    def add_segment_uuid(self, segment, ids):
        """ Add segment uuid to Elasticsearch """
        segment.es_manager.add_to_segment_by_ids(ids, segment.uuid)

    def get_source_list(self, segment):
        """ Create set of source list urls from segment export file """
        source_ids = set(segment.s3.get_extract_export_ids(segment.source.filename))
        return source_ids

    @property
    def serializer(self):
        if self.segment.segment_type in (0, "video"):
            serializer = self._get_video_serializer()
        else:
            serializer = self._get_channel_serializer()
        return serializer

    def _get_video_serializer(self):
        if self._vetting is True:
            serializer = CustomSegmentVideoVettedExportSerializer
        else:
            serializer = CustomSegmentVideoExportSerializer
        return serializer

    def _get_channel_serializer(self):
        if self._vetting is True:
            serializer = CustomSegmentChannelVettedExportSerializer
        else:
            owner = getattr(self.segment, "owner", None)
            if owner and owner.has_perm("userprofile.monetization_filter"):
                serializer = CustomSegmentChannelWithMonetizationExportSerializer
            else:
                serializer = CustomSegmentChannelExportSerializer
        return serializer
