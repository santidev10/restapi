from collections import defaultdict
import csv
import logging
import os
import tempfile

from django.conf import settings

from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from segment.utils.bulk_search import bulk_search
from utils.brand_safety import map_brand_safety_score

BATCH_SIZE = 5000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
MONETIZATION_SORT = {f"{Sections.MONETIZATION}.is_monetizable": "desc"}

logger = logging.getLogger(__name__)


def generate_segment(segment, query, size, sort=None, options=None):
    """
    :param segment: CustomSegment | PersistentSegment
    :param query: dict
    :param size: int
    :param sort: list -> Additional sort fields
    :return:
    """
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        sort = sort or [segment.SORT_KEY]
        seen = 0
        item_ids = []
        top_three_items = []
        segment_item_ids = []
        aggregations = defaultdict(int)

        if segment.segment_type == 0 or segment.segment_type == "video":
            cursor_field = "stats.views"
        else:
            cursor_field = "stats.subscribers"
            if options is None:
                options = [
                    QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
                    QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
                ]

        for batch in bulk_search(segment.es_manager.model, query, sort, cursor_field, options=options, batch_size=5000, source=segment.SOURCE_FIELDS):
            with open(filename, mode="a", newline="") as file:
                fieldnames = segment.serializer.columns
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                if seen == 0:
                    writer.writeheader()

                for item in batch:
                    if len(top_three_items) < 3 \
                            and getattr(item.general_data, "title", None) \
                            and getattr(item.general_data, "thumbnail_image_url", None):
                        top_three_items.append({
                            "id": item.main.id,
                            "title": item.general_data.title,
                            "image_url": item.general_data.thumbnail_image_url
                        })

                    if item.general_data:
                        segment_item_ids.append(item.main.id)

                    item_ids.append(item.main.id)
                    row = segment.serializer(item).data
                    writer.writerow(row)

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
            if seen >= size:
                break

        # Average fields
        aggregations["average_brand_safety_score"] = map_brand_safety_score(aggregations["average_brand_safety_score"] // (seen or 1))
        aggregations["ctr"] /= seen or 1
        aggregations["ctr_v"] /= seen or 1
        aggregations["video_view_rate"] /= seen or 1
        aggregations["average_cpm"] /= seen or 1
        aggregations["average_cpv"] /= seen or 1

        segment.es_manager.add_to_segment_by_ids(item_ids[:DOCUMENT_SEGMENT_ITEMS_SIZE], segment.uuid)
        statistics = {
            "items_count": seen,
            "top_three_items": top_three_items,
            **aggregations,
        }
        s3_key = segment.get_s3_key()
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

