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
from utils.brand_safety import map_brand_safety_score
import csv
import logging
import os
import tempfile

BATCH_SIZE = 5000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
MONETIZATION_SORT = {f"{Sections.MONETIZATION}.is_monetizable": "desc"}

logger = logging.getLogger(__name__)


def generate_segment(segment, query, size, sort=None, options=None, add_uuid=True, s3_key=None):
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
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    context = prepare_context()
    try:
        sort = sort or [segment.SORT_KEY]
        seen = 0
        item_ids = []
        top_three_items = []
        aggregations = defaultdict(int)

        # If video, retrieve videos ordered by views
        if segment.segment_type == 0 or segment.segment_type == "video":
            cursor_field = VIEWS_FIELD
            # Exclude all age_restricted items
            if options is None:
                options = [
                    QueryBuilder().build().must().term().field("general_data.age_restricted").value(False).get()
                ]
        else:
            cursor_field = SUBSCRIBERS_FIELD
            # If channel, retrieve is_monetizable channels first then non-is_monetizable channels
            # for is_monetizable channel items to appear first on export
            if options is None:
                options = [
                    QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
                    QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
                ]
        try:
            for batch in bulk_search(segment.es_manager.model, query, sort, cursor_field, options=options,
                                     batch_size=5000, source=segment.SOURCE_FIELDS, include_cursor_exclusions=True):
                # Retrieve Postgres vetting data for vetting exports
                # no longer need to check if vetted for this, as this data is being used on all exports
                batch_item_ids = [item.main.id for item in batch]
                item_ids.extend(batch_item_ids)
                try:
                    vetting = AuditUtils.get_vetting_data(
                        segment.audit_utils.vetting_model, segment.audit_id, batch_item_ids, segment.data_field
                    )
                except Exception as e:
                    logger.warning(f"Error getting segment extra data: {e}")
                    vetting = {}

                context["vetting"] = vetting
                with open(filename, mode="a", newline="") as file:
                    fieldnames = segment.serializer.columns
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    if seen == 0:
                        writer.writeheader()

                    for item in batch:
                        # YT_GENRE_CHANNELS have no data and should not be on any export
                        if item.main.id in YT_GENRE_CHANNELS:
                            continue
                        if len(top_three_items) < 3 \
                                and getattr(item.general_data, "title", None) \
                                and getattr(item.general_data, "thumbnail_image_url", None):
                            top_three_items.append({
                                "id": item.main.id,
                                "title": item.general_data.title,
                                "image_url": item.general_data.thumbnail_image_url
                            })

                        row = segment.serializer(item, context=context).data
                        writer.writerow(row)

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

                        if segment.segment_type == 0 or segment.segment_type == "video":
                            aggregations["likes"] += item.stats.likes or 0
                            aggregations["dislikes"] += item.stats.dislikes or 0
                        else:
                            aggregations["likes"] += item.stats.observed_videos_likes or 0
                            aggregations["dislikes"] += item.stats.observed_videos_dislikes or 0
                            aggregations["monthly_subscribers"] += item.stats.last_30day_subscribers or 0
                            aggregations["subscribers"] += item.stats.subscribers or 0
                            aggregations["audited_videos"] += item.brand_safety.videos_scored or 0

                        seen += 1
                        # Number of results from bulk_search is imprecise
                        if seen >= size:
                            raise MaxItemsException
        except MaxItemsException:
            pass

        # Average fields
        aggregations["average_brand_safety_score"] = map_brand_safety_score(aggregations["average_brand_safety_score"] // (seen or 1))
        aggregations["ctr"] /= seen or 1
        aggregations["ctr_v"] /= seen or 1
        aggregations["video_view_rate"] /= seen or 1
        aggregations["average_cpm"] /= seen or 1
        aggregations["average_cpv"] /= seen or 1

        if add_uuid:
            segment.es_manager.add_to_segment_by_ids(item_ids[:DOCUMENT_SEGMENT_ITEMS_SIZE], segment.uuid)
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


def prepare_context():
    brand_safety_categories = {
        category.id: category.name
        for category in BadWordCategory.objects.all()
    }
    context = {
        "brand_safety_categories": brand_safety_categories,
        "age_groups": AuditAgeGroup.to_str,
        "genders": AuditGender.to_str,
        "content_types": AuditContentType.to_str,
    }
    return context
