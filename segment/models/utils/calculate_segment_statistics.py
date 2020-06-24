from collections import defaultdict

from django.conf import settings

from es_components.constants import Sections
from segment.models.persistent.constants import PersistentSegmentType
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics


def calculate_statistics(segment, items=None, es_query=None):
    """
    Aggregate Google Ads statistics with documents returned in es_query
    :param segment: Segment object
    :param items: Elasitcsearch documents
    :param es_query: Query object
    :return:
    """
    related_aw_statistics_model = segment.related_aw_statistics_model
    segment_type = segment.segment_type

    if segment_type == PersistentSegmentType.VIDEO or segment_type == 0:
        sort = ("-stats.views",)
        segment_aggregation_type = 0
    elif segment_type == PersistentSegmentType.CHANNEL or segment_type == 1:
        sort = ("-stats.subscribers",)
        segment_aggregation_type = 1
    else:
        raise ValueError(f"Unsupported segment type: {segment_type}")

    # Query for aggregations and documents to process
    if items is None:
        if es_query is None:
            es_query = segment.get_segment_items_query()
        search = segment.es_manager.search(query=es_query, limit=settings.MAX_SEGMENT_TO_AGGREGATE)
        search = add_aggregations_with_sort(search, sort, segment_type=segment_aggregation_type)
        result = search.execute()
        items = result.hits
        aggregations = extract_aggregations(result.aggregations.to_dict())
        items_count = result.hits.total.value
    # Process provided documents
    else:
        # Extract aggregations from items
        aggregations = calculate_aggregations(items, segment_type=segment_aggregation_type)
        items_count = len(items)

    top_three_items = []
    all_ids = []
    for doc in items:
        all_ids.append(doc.main.id)
        # Check if we data to display for each item in top three
        if len(top_three_items) < 3 \
            and getattr(doc.general_data, "title", None) \
            and getattr(doc.general_data, "thumbnail_image_url", None):
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
        results[key] = int(value["value"])
    return results


def add_aggregations_with_sort(search_obj, sort, segment_type=0):
    """
    Calculate aggregations given search object to query Elasticsearch
    :param search_obj:
    :param sort: str
    :param segment_type: int
    :return: search_obj
    """
    search_obj = search_obj.sort(*sort)
    search_obj.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")
    search_obj.aggs.bucket("monthly_views", "sum", field=f"{Sections.STATS}.last_30day_views")
    search_obj.aggs.bucket("average_brand_safety_score", "avg", field=f"{Sections.BRAND_SAFETY}.overall_score")
    if segment_type == 0:
        search_obj.aggs.bucket("likes", "sum", field=f"{Sections.STATS}.likes")
        search_obj.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.dislikes")
    elif segment_type == 1:
        search_obj.aggs.bucket("likes", "sum", field=f"{Sections.STATS}.observed_videos_likes")
        search_obj.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.observed_videos_dislikes")
        search_obj.aggs.bucket("subscribers", "sum", field=f"{Sections.STATS}.subscribers")
        search_obj.aggs.bucket("monthly_subscribers", "sum", field=f"{Sections.STATS}.last_30day_subscribers")
        search_obj.aggs.bucket("audited_videos", "sum", field=f"{Sections.BRAND_SAFETY}.videos_scored")
    else:
        raise ValueError(f"Unsupported segment type: {segment_type}")
    return search_obj


def calculate_aggregations(items, segment_type=0):
    """
    Calculate aggregations given list of document items
    :param items: iterable
    :param segment_type: int
    :return: dict
    """
    aggregations = defaultdict(int)
    for item in items:
        aggregations["views"] += item.stats.views or 0
        aggregations["monthly_views"] += item.stats.last_30day_views or 0
        aggregations["average_brand_safety_score"] += item.brand_safety.overall_score or 0

        if segment_type == 0:
            aggregations["likes"] += item.stats.likes or 0
            aggregations["dislikes"] += item.stats.dislikes or 0
        elif segment_type == 1:
            aggregations["likes"] += item.stats.observed_videos_likes or 0
            aggregations["dislikes"] += item.stats.observed_videos_dislikes or 0
            aggregations["monthly_subscribers"] += item.stats.last_30day_subscribers or 0
            aggregations["subscribers"] += item.stats.subscribers or 0
            aggregations["audited_videos"] += item.brand_safety.videos_scored or 0
        else:
            raise ValueError(f"Unsupported segment type: {segment_type}")
    aggregations["average_brand_safety_score"] = aggregations["average_brand_safety_score"] // (len(items) or 1)
    return aggregations
