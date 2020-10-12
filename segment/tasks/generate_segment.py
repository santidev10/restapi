import logging
import os
import tempfile
from collections import defaultdict

from django.conf import settings
from elasticsearch_dsl import Q

from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from es_components.managers import ChannelManager
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import SourceListType
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from segment.utils.bulk_search import bulk_search
from segment.utils.utils import get_content_disposition


BATCH_SIZE = 5000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
SOURCE_SIZE_GET_LIMIT = 10000

logger = logging.getLogger(__name__)


# pylint: disable=too-many-nested-blocks,too-many-statements
def generate_segment(segment, query, size, sort=None, s3_key=None, options=None, add_uuid=False, with_audit=False):
    """
    Helper method to create segments
        Options determine additional filters to apply sequentially when retrieving items
        If None and for channels, first retrieves is_monetizable then non-is_monetizable items
    :param segment: CustomSegment | PersistentSegment
    :param query: dict
    :param size: int
    :param sort: list -> Additional sort fields
    :param add_uuid: Add uuid to document segments section
    :return:
    """
    generate_utils = GenerateSegmentUtils(segment)
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    context = generate_utils.default_serialization_context
    source_list = None
    source_type = None
    # pylint: disable=broad-except
    try:
        source_list = generate_utils.get_source_list(segment)
        source_type = segment.source.source_type
    except (AttributeError, CustomSegmentSourceFileUpload.DoesNotExist):
        pass
    except Exception:
        logger.exception("Error trying to retrieve source list for "
                         "segment: %s, segment_type: %s", segment.title, segment.segment_type)
    try:
        sort = sort or [segment.config.SORT_KEY]
        seen = 0
        item_ids = []
        top_three_items = []
        aggregations = defaultdict(int)
        default_search_config = generate_utils.default_search_config
        # Must use bool flag to determine if we should write header instead of seen. If there is a source_list, then
        # a batch may be empty since we check set membership for the current bulk_search batch in the source list
        write_header = True
        if options is None:
            options = default_search_config["options"]
        try:
            # Use query by ids along with filters to avoid requesting entire database with bulk_search
            if source_list and len(source_list) <= SOURCE_SIZE_GET_LIMIT:
                ids_query = QueryBuilder().build().must().terms().field('main.id').value(list(source_list)).get()
                full_query = Q(query) + ids_query
                es_generator = segment.es_manager.search(query=full_query.to_dict())
                es_generator = [es_generator.execute().hits]
            else:
                bulk_search_kwargs = dict(
                    options=options, batch_size=5000, include_cursor_exclusions=True
                )
                es_generator = bulk_search_with_source_generator(
                    source_list, source_type,
                    segment.es_manager.model, query, sort, default_search_config["cursor_field"],
                    **bulk_search_kwargs)

            for batch in es_generator:
                # Clean blocklist items
                batch = generate_utils.clean_blocklist(batch, segment.segment_type)
                # Ensure that we are not adding items past limit
                batch = batch[:size - seen]
                batch_item_ids = [item.main.id for item in batch]
                item_ids.extend(batch_item_ids)
                # Get the current batch's Postgres vetting data context for serialization
                vetting = generate_utils.get_vetting_data(segment, batch_item_ids)
                context["vetting"] = vetting
                generate_utils.write_to_file(batch, filename, segment, context, aggregations,
                                             write_header=write_header is True)
                generate_utils.add_aggregations(aggregations, batch)
                seen += len(batch_item_ids)
                write_header = False
                if seen >= size:
                    raise MaxItemsException
        except MaxItemsException:
            pass
        generate_utils.finalize_aggregations(aggregations, seen)
        if add_uuid is True:
            generate_utils.add_segment_uuid(segment, item_ids[:DOCUMENT_SEGMENT_ITEMS_SIZE])
        statistics = {
            "items_count": seen,
            "top_three_items": top_three_items,
            **aggregations,
        }
        s3_key = s3_key or segment.get_s3_key()
        if with_audit is True:
            segment.s3.export_file_to_s3(filename, s3_key)
            # CTL export csv is finished, start audit for further filtering with inclusion_file / exclusion
            # file keywords
            generate_utils.start_audit(filename)
            results = {}
        else:
            content_disposition = get_content_disposition(segment, is_vetting=getattr(segment, "is_vetting", False))
            segment.s3.export_file_to_s3(filename, s3_key, extra_args={"ContentDisposition": content_disposition})
            download_url = segment.s3.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
            results = {
                "statistics": statistics,
                "download_url": download_url,
                "s3_key": s3_key,
            }
        return results

    finally:
        os.remove(filename)
# pylint: enable=too-many-nested-blocks,too-many-statements


def bulk_search_with_source_generator(source_list, source_type, model, query, sort, cursor_field, **bulk_search_kwargs):
    """
    Wrapper to check source list for each batch in bulk search generator
    :param source_list: iter: Source list upload
    :param source_type: int
    :param model: es_components.models.Model
    :param query: ES Q object
    :param sort: str
    :param cursor_field: str
    :param bulk_search_kwargs:
    :return:
    """
    bulk_search_generator = bulk_search(model, query, sort, cursor_field, **bulk_search_kwargs)
    for batch in bulk_search_generator:
        if source_list:
            if source_type == SourceListType.INCLUSION.value:
                batch = [item for item in batch if item.main.id in source_list]
            else:
                batch = [item for item in batch if item.main.id not in source_list]
        yield batch


def _clean_blocklist(items, data_type=0):
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


class MaxItemsException(Exception):
    pass
