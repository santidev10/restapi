import csv
from collections import defaultdict
import os
import tempfile

from django.conf import settings

from saas import celery_app
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics
from utils.brand_safety import map_brand_safety_score
from utils.utils import chunks_generator

BATCH_SIZE = 1000
DOCUMENT_SEGMENT_ITEMS_SIZE = 100
STATISTICS_IDS_SIZE = 200


def generate_segment(segment, query, size):
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        scan = segment.es_manager.model.search().query(query).sort(segment.SORT_KEY).source(segment.SOURCE_FIELDS).params(preserve_order=True).scan()
        seen = 0
        item_ids = []
        top_three_items = []
        # Documents to update with segment uuid, only used for preview
        document_segment_items = []
        aggregations = defaultdict(int)

        for batch in chunks_generator(scan, size=BATCH_SIZE):
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

                    if len(document_segment_items) < DOCUMENT_SEGMENT_ITEMS_SIZE:
                        document_segment_items.append(item.main.id)

                    item_ids.append(item.main.id)
                    row = segment.serializer(item).data
                    writer.writerow(row)

                    aggregations["monthly_views"] += item.stats.last_30day_views or 0
                    aggregations["average_brand_safety_score"] += item.brand_safety.overall_score or 0

                    if segment.segment_type == 0 or segment.segment_type == "video":
                        aggregations["views"] += item.stats.views or 0
                        aggregations["likes"] += item.stats.likes or 0
                        aggregations["dislikes"] += item.stats.dislikes or 0
                    else:
                        aggregations["views"] += item.stats.observed_videos_views or 0
                        aggregations["likes"] += item.stats.observed_videos_likes or 0
                        aggregations["dislikes"] += item.stats.observed_videos_dislikes or 0
                        aggregations["monthly_subscribers"] += item.stats.last_30day_subscribers or 0
                        aggregations["subscribers"] += item.stats.subscribers or 0
                        aggregations["audited_videos"] += item.brand_safety.videos_scored or 0

                    seen += 1
            if seen >= size:
                break
        aggregations["average_brand_safety_score"] = map_brand_safety_score(aggregations["average_brand_safety_score"] // (seen or 1))
        aggregated_statistics = aggregate_segment_statistics(segment.related_aw_statistics_model, item_ids[:STATISTICS_IDS_SIZE])

        # segment.es_manager.add_to_segment_by_ids(document_segment_items, segment.uuid)
        statistics = {
            "items_count": seen,
            "top_three_items": top_three_items,
            **aggregations,
            **aggregated_statistics,
        }
        s3_key = segment.get_s3_key()
        # segment.s3_exporter.export_file_to_s3(filename, s3_key)
        download_url = segment.s3_exporter.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        results = {
            "statistics": statistics,
            "download_url": download_url
        }
        return results

    except Exception as e:
        print(e)

    finally:
        os.remove(filename)

