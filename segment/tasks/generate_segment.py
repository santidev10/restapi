import logging
import os
import tempfile
from collections import defaultdict

from django.conf import settings

from es_components.constants import Sections
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import SourceListType
from segment.utils.bulk_search import bulk_search
from segment.utils.generate_segment_utils import GenerateSegmentUtils

BATCH_SIZE = 5000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
SOURCE_SIZE_GET_LIMIT = 10000
MONETIZATION_SORT = {f"{Sections.MONETIZATION}.is_monetizable": "desc"}

logger = logging.getLogger(__name__)


# pylint: disable=too-many-nested-blocks,too-many-statements
def generate_segment(segment, query, size, sort=None, options=None, add_uuid=False, s3_key=None):
    """
    Helper method to create segments
        Options determine additional filters to apply sequentially when retrieving items
        If None and for channels, first retrieves is_monetizable then non-is_monetizable items
    :param segment: CustomSegment | PersistentSegment
    :param query: dict
    :param size: int
    :param sort: list -> Additional sort fields
    :param add_uuid: Add uuid to document segments section
    :param s3_key: Optional s3 key for file upload
    :return:
    """
    generate_utils = GenerateSegmentUtils()
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    context = generate_utils.get_default_serialization_context()
    source_list = None
    source_type = None
    # pylint: disable=broad-except
    try:
        source_list = generate_utils.get_source_list(segment)
        source_type = segment.source.source_type
    except CustomSegmentSourceFileUpload.DoesNotExist:
        pass
    except Exception:
        logger.exception("Error trying to retrieve source list for "
                         "segment: %s, segment_type: %s", segment.title, segment.segment_type)
    try:
        sort = sort or [segment.SORT_KEY]
        seen = 0
        item_ids = []
        top_three_items = []
        aggregations = defaultdict(int)
        default_search_config = generate_utils.get_default_search_config(segment.segment_type)
        # Must use bool flag to determine if we should write header instead of seen. If there is a source_list, then
        # a batch may be empty since we check set membership for the current bulk_search batch in the source list
        write_header = True
        if options is None:
            options = default_search_config["options"]
        try:
            if source_list and len(source_list) <= SOURCE_SIZE_GET_LIMIT:
                es_generator = [segment.es_manager.get(source_list, skip_none=True)]
            else:
                es_generator = bulk_search(segment.es_manager.model, query, sort, default_search_config["cursor_field"],
                                           options=options, batch_size=5000, source=segment.SOURCE_FIELDS,
                                           include_cursor_exclusions=True)
            for batch in es_generator:
                if source_list:
                    if source_type == SourceListType.INCLUSION.value:
                        batch = [item for item in batch if item.main.id in source_list]
                    else:
                        batch = [item for item in batch if item.main.id not in source_list]
                batch = batch[:size - seen]
                batch_item_ids = [item.main.id for item in batch]
                item_ids.extend(batch_item_ids)
                vetting = generate_utils.get_vetting_data(segment, batch_item_ids)
                context["vetting"] = vetting
                generate_utils.write_to_file(batch, filename, segment, context, aggregations,
                                             write_header=write_header is True)
                generate_utils.add_aggregations(aggregations, batch, segment.segment_type)
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
        s3_key = segment.get_s3_key() if s3_key is None else s3_key
        segment.s3_exporter.export_file_to_s3(filename, s3_key)
        download_url = segment.s3_exporter.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        results = {
            "statistics": statistics,
            "download_url": download_url,
            "s3_key": s3_key,
        }
        return results

    finally:
        os.remove(filename)
# pylint: enable=too-many-nested-blocks,too-many-statements

class MaxItemsException(Exception):
    pass
