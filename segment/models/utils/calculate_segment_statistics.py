from collections import defaultdict

from django.conf import settings

from es_components.constants import Sections
from segment.models.persistent.constants import PersistentSegmentType
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics


def calculate_statistics(segment, items=None, es_query=None):
    """
    Aggregate Google Ads statistics with documents returned in es_query
    :param segment: Segment object
    :return:
    """
    related_aw_statistics_model = segment.related_aw_statistics_model
    segment_type = segment.segment_type
    # Query for aggregations and documents to process
    if items is None:
        if es_query is None:
            es_query = segment.get_segment_items_query()
        search = segment.es_manager.search(query=es_query, limit=settings.MAX_SEGMENT_TO_AGGREGATE)
        if segment_type == PersistentSegmentType.VIDEO or segment_type == 0:
            search = search.sort(*("-stats.views",))
            add_video_aggregation_filters(search)
        elif segment_type == PersistentSegmentType.CHANNEL or segment_type == 1:
            search = search.sort(*("-stats.subscribers",))
            add_channel_aggregation_filters(search)
        else:
            raise ValueError(f"Unsupported segment type: {segment_type}")
        result = search.execute()
        items = result.hits
        aggregations = extract_aggregations(result.aggregations.to_dict())
        items_count = result.hits.total
    # Process provided documents
    else:
        if segment_type == PersistentSegmentType.VIDEO or segment_type == 0:
            handler = get_video_aggregations
        elif segment_type == PersistentSegmentType.CHANNEL or segment_type == 1:
            handler = get_channel_aggregations
        else:
            raise ValueError(f"Unsupported segment type: {segment_type}")
        # Extract aggregations from items
        aggregations = handler(items)
        items_count = len(items)

    top_three_items = []
    all_ids = []
    for doc in items:
        all_ids.append(doc.main.id)
        # Check if we data to display for each item in top three
        if len(top_three_items) < 3 and getattr(doc.general_data, "title", None) and getattr(doc.general_data,
                                                                                             "thumbnail_image_url",
                                                                                             None):
            top_three_items.append({
                "id": doc.main.id,
                "title": doc.general_data.title,
                "image_url": doc.general_data.thumbnail_image_url
            })

    aggregated_statistics = aggregate_segment_statistics(related_aw_statistics_model, all_ids)
    statistics = {
        "items_count": items_count,
        "top_three_items": top_three_items,
        **aggregations,
        **aggregated_statistics,
    }
    return statistics


def extract_aggregations(aggregation_result_dict):
    """
    Extract value fields of aggregation results
    :param aggregation_result_dict: { "agg_name" : { value: "a_value" } }
    :return:
    """
    results = {}
    for key, value in aggregation_result_dict.items():
        results[key] = value["value"]
    return results


def add_video_aggregation_filters(search_obj):
    search_obj.aggs.bucket("likes", "sum", field=f"{Sections.STATS}.likes")
    search_obj.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.dislikes")
    search_obj.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")


def add_channel_aggregation_filters(search_obj):
    search_obj.aggs.bucket("likes", "sum", field=f"{Sections.STATS}.observed_videos_likes")
    search_obj.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.observed_videos_dislikes")
    search_obj.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")
    search_obj.aggs.bucket("subscribers", "sum", field=f"{Sections.STATS}.subscribers")
    search_obj.aggs.bucket("audited_videos", "sum", field=f"{Sections.BRAND_SAFETY}.videos_scored")


def get_video_aggregations(items):
    aggregations = defaultdict(int)
    for item in items:
        aggregations["likes"] += item.stats.likes or 0
        aggregations["dislikes"] += item.stats.dislikes or 0
        aggregations["views"] += item.stats.views or 0
    return aggregations


def get_channel_aggregations(items):
    aggregations = defaultdict(int)
    for item in items:
        aggregations["likes"] += item.stats.observed_videos_likes or 0
        aggregations["dislikes"] += item.stats.observed_videos_dislikes or 0
        aggregations["views"] += item.stats.views or 0
        aggregations["subscribers"] += item.stats.subscribers or 0
        aggregations["audited_videos"] += item.brand_safety.videos_scored or 0
    return aggregations
