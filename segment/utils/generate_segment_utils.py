from enum import Enum

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.models import BadWordCategory
from collections import defaultdict
from django.conf import settings
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import Sections
from es_components.constants import VIEWS_FIELD
from es_components.query_builder import QueryBuilder
from segment.models.persistent.constants import YT_GENRE_CHANNELS
from segment.utils.bulk_search import bulk_search
from segment.utils.write_file import write_file
from utils.brand_safety import map_brand_safety_score
import csv
import logging
import os
import tempfile


class GenerateSegmentUtils:
    _default_context = None

    @staticmethod
    def get_vetting_data(segment, item_ids):
        # Retrieve Postgres vetting data for vetting exports
        # no longer need to check if vetted for this, as this data is being used on all exports
        try:
            vetting = AuditUtils.get_vetting_data(
                segment.audit_utils.vetting_model, segment.audit_id, item_ids, segment.data_field
            )
        except Exception as e:
            vetting = {}
        return vetting

    def get_default_search_config(self, segment_type):
        if segment_type == 0 or segment_type == "video":
            config = self._default_video_search_config
        elif segment_type == 1 or segment_type == "channel":
            config = self._default_channel_search_config
        else:
            raise ValueError(f"Invalid segment_type: {segment_type}")
        return config

    def get_default_serialization_context(self):
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

    @property
    def _default_video_search_config(self):
        config = dict(
            cursor_field=VIEWS_FIELD,
            # Exclude all age_restricted items
            options=[
                QueryBuilder().build().must().term().field("general_data.age_restricted").value(False).get()
            ]
        )
        return config

    @property
    def _default_channel_search_config(self):
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

            if segment_type == 0 or segment_type == "video":
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
        # Average fields
        aggregations["average_brand_safety_score"] = map_brand_safety_score(
            aggregations["average_brand_safety_score"] // (count or 1))
        aggregations["ctr"] /= count or 1
        aggregations["ctr_v"] /= count or 1
        aggregations["video_view_rate"] /= count or 1
        aggregations["average_cpm"] /= count or 1
        aggregations["average_cpv"] /= count or 1
        return aggregations

    def add_segment_uuid(self, segment, ids):
        segment.es_manager.add_to_segment_by_ids(ids, segment.uuid)
