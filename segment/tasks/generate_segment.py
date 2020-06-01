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
from segment.utils.generate_segment_utils import GenerateSegmentUtils

BATCH_SIZE = 5000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
MONETIZATION_SORT = {f"{Sections.MONETIZATION}.is_monetizable": "desc"}

logger = logging.getLogger(__name__)


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
    :param get_exists_params: dict -> Parameters to pass to get_exists util method
    :return:
    """
    generate_utils = GenerateSegmentUtils()

    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    context = generate_utils.get_default_serialization_context()
    try:
        sort = sort or [segment.SORT_KEY]
        seen = 0
        item_ids = []
        top_three_items = []
        aggregations = defaultdict(int)

        default_search_config = generate_utils.get_default_search_config(segment.segment_type)
        if options is None:
            options = default_search_config["options"]
        try:
            for batch in bulk_search(segment.es_manager.model, query, sort, default_search_config["cursor_field"],
                                     options=options, batch_size=5000, source=segment.SOURCE_FIELDS, include_cursor_exclusions=True):
                batch = batch[:size - seen]
                batch_item_ids = [item.main.id for item in batch]
                item_ids.extend(batch_item_ids)
                vetting = generate_utils.get_vetting_data(segment, batch_item_ids)
                context["vetting"] = vetting
                write_file(batch, filename, segment, context, aggregations, write_header=seen == 0)
                generate_utils.add_aggregations(aggregations, batch, segment.segment_type)
                seen += len(batch_item_ids)
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

    except Exception:
        raise

    finally:
        os.remove(filename)


class MaxItemsException(Exception):
    pass