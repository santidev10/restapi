import csv
from operator import attrgetter
import os
import tempfile
from collections import defaultdict

from django.conf import settings
from uuid import uuid4

from .constants import channel_sum_aggs
from .constants import shared_avg_aggs
from .constants import video_sum_aggs
from audit_tool.models import AuditProcessor
from audit_tool.api.views.audit_save import AuditFileS3Exporter
from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.models import BadWordCategory
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import Sections
from es_components.constants import VIEWS_FIELD
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from segment.models.persistent.constants import YT_GENRE_CHANNELS
from utils.brand_safety import map_brand_safety_score
from utils.utils import chunks_generator


class GenerateSegmentUtils:
    _default_context = None
    segment = None

    def __init__(self, segment):
        self.segment_type = segment.segment_type
        self.segment = segment
        self.avg_aggs = shared_avg_aggs
        if self.segment_type in {0, "video"}:
            self.sum_aggs = video_sum_aggs
        else:
            self.sum_aggs = channel_sum_aggs

    @staticmethod
    def get_vetting_data(segment, item_ids):
        """ Retrieve Postgres vetting data for serialization """
        try:
            vetting = AuditUtils.get_vetting_data(
                segment.audit_utils.vetting_model, segment.audit_id, item_ids, segment.config.DATA_FIELD
            )
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            vetting = {}
        return vetting

    @property
    def default_search_config(self):
        """ Get default bulk search function config depending on segment type """
        segment_type = self.segment.segment_type
        if segment_type in (0, "video"):
            config = self._get_default_video_search_config()
        else:
            config = self._get_default_channel_search_config()
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
                for category in BadWordCategory.objects.filter(vettable=True)
            }
            self._default_context = context = {
                "brand_safety_categories": brand_safety_categories,
                "age_groups": AuditAgeGroup.to_str,
                "genders": AuditGender.to_str,
                "content_types": AuditContentType.to_str,
                "quality_types": AuditContentQuality.to_str,
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

    def write_to_file(self, items, filename, segment, serializer_context, write_header=False, mode="a"):
        """ Write data to csv file """
        rows = []
        fieldnames = segment.export_serializer.columns
        for item in items:
            # YT_GENRE_CHANNELS have no data and should not be on any export
            if item.main.id in YT_GENRE_CHANNELS:
                continue
            row = segment.export_serializer(item, context=serializer_context).data
            rows.append(row)
        with open(filename, mode=mode, newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore", quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            if write_header is True:
                writer.writeheader()
            writer.writerows(rows)

    def add_aggregations(self, aggregations, items):
        """
        Add values to aggregations
        Aggregations are performed in Python as calculating in Elasticsearch appears to overload memory usage
        """
        # Calculating aggregations with each items already retrieved is much more efficient than
        # executing an additional aggregation query
        for item in items:
            for agg in self.avg_aggs + self.sum_aggs:
                key = agg.split(".")[1]
                value = attrgetter(agg)(item) or 0
                aggregations[key] += value

    def finalize_aggregations(self, aggregations, count):
        """ Finalize aggregations and calculate averages"""
        for agg in self.avg_aggs:
            key = agg.split(".")[1]
            aggregations[key] /= count or 1
        aggregations["overall_score"] = map_brand_safety_score(aggregations["overall_score"])
        # Map channel keys
        map_keys = (
            ("observed_videos_likes", "likes"),
            ("observed_videos_dislikes", "dislikes")
        )
        for old_key, new_key in map_keys:
            try:
                aggregations[new_key] = aggregations[old_key]
                del aggregations[old_key]
            except KeyError:
                pass
        return aggregations

    def add_segment_uuid(self, segment, ids):
        """ Add segment uuid to Elasticsearch """
        segment.es_manager.add_to_segment_by_ids(ids, segment.uuid)

    def get_source_list(self, segment):
        """ Create set of source list urls from segment export file """
        source_ids = set(segment.s3.get_extract_export_ids(segment.source.filename))
        return source_ids

    @staticmethod
    def clean_blocklist(items, data_type=0):
        """
        Remove videos that have their channel blocklisted
        :param items:
        :param data_type: int -> 0 = videos, 1 = channels
        :return:
        """
        channel_manager = ChannelManager([Sections.CUSTOM_PROPERTIES])
        if data_type == 0:
            channels = channel_manager.get([video.channel.id for video in items if video.channel.id is not None])
            blocklist = {
                channel.main.id: channel.custom_properties.blocklist
                for channel in channels
            }
            non_blocklist = [
                video for video in items if blocklist.get(video.channel.id) is not True
                and video.custom_properties.blocklist is not True
            ]
        else:
            non_blocklist = [
                channel for channel in items if channel.custom_properties.blocklist is not True
            ]
        return non_blocklist

    def start_audit(self, filename):
        """
        Upload audit source channel / video urls file and make audit visible for processing
        :param filename: str -> On disk fp of export file
        :return:
        """
        audit = AuditProcessor.objects.get(id=self.segment.params["meta_audit_id"])
        self._upload_audit_source_file(audit, filename)
        # Update audit.temp_stop to make it visible for processing
        audit.temp_stop = False
        audit.save()

    def _upload_audit_source_file(self, audit, source_fp):
        """
        Create source urls file for audit processing
        :param audit: AuditProcessor
        :param source_fp: str
        :return:
        """
        source_urls_file = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
        with open(source_fp, mode="r") as source_file,\
                open(source_urls_file, mode="w") as dest_file:
            reader = csv.reader(source_file)
            writer = csv.writer(dest_file)
            # Skip header and only use urls as source
            next(reader)
            for chunk in chunks_generator(reader, size=1000):
                rows = [[row[0]] for row in chunk]
                writer.writerows(rows)
        name = uuid4().hex
        AuditFileS3Exporter.export_file_to_s3(source_urls_file, name)
        audit.params["seed_file"] = name
        audit.save()
        os.remove(source_urls_file)

    def get_aggregations_by_ids(self, ids):
        """
        Convenience method to retrieve documents and manually calculate aggregations
        If number of ids is too large, calculating aggregations by ids query is increasingly slow
        """
        aggregations = defaultdict(int)
        for chunk in chunks_generator(ids, size=10000):
            items = self.segment.es_manager.get(chunk, skip_none=True)
            self.add_aggregations(aggregations, items)
        self.finalize_aggregations(aggregations, len(ids))
        return aggregations

    def get_aggregations_by_query(self, query):
        """
        Calculate Elasticsearch aggregations by query
        :param query: Q object
        :return:
        """
        search = self.segment.es_manager.search(filters=query, limit=0)
        for agg in self.avg_aggs:
            metric_name = agg.split(".")[-1]
            search.aggs.metric(metric_name, "avg", field=agg, missing=0)
        for agg in self.sum_aggs:
            metric_name = agg.split(".")[-1]
            search.aggs.metric(metric_name, "sum", field=agg, missing=0)
        agg_result = search.execute()
        aggregations = {}
        for agg in self.avg_aggs + self.sum_aggs:
            metric_name = agg.split(".")[-1]
            aggregations[metric_name] = getattr(agg_result.aggregations, metric_name).value
        return aggregations
