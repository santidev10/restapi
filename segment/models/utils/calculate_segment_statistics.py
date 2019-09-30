from django.conf import settings

from segment.models.persistent.constants import PersistentSegmentType
from es_components.constants import Sections
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics


def calculate_statistics(related_aw_statistics_model, segment_type, es_manager, es_query):
    """
    Aggregate Google Ads statistics with documents returned in es_query
    :param segment: Segment object
    :param es_manager: es_components Manager object
    :param es_query: Elasticsearch DSL Q object or JSON query
    :return:
    """
    search = es_manager.search(query=es_query)
    search.aggs.bucket("likes", "sum", field=f"{Sections.STATS}.observed_videos_likes")
    search.aggs.bucket("dislikes", "sum", field=f"{Sections.STATS}.observed_videos_dislikes")
    search.aggs.bucket("views", "sum", field=f"{Sections.STATS}.views")

    # Additional aggregations for channel segments
    if segment_type == PersistentSegmentType.CHANNEL or segment_type == 1:
        sort = ("-stats.subscribers",)
        search.aggs.bucket("subscribers", "sum", field=f"{Sections.STATS}.subscribers")
        search.aggs.bucket("audited_videos", "sum", field=f"{Sections.BRAND_SAFETY}.videos_scored")
    else:
        sort = ("-stats.views",)
    result = search.execute()
    aggregations = extract_aggregations(result.aggregations.to_dict())
    items_count = result.hits.total

    items_for_statistics_result = es_manager.search(es_query, sort=sort, limit=settings.MAX_SEGMENT_TO_AGGREGATE).execute()
    top_three_items = []
    all_ids = []
    for doc in items_for_statistics_result.hits:
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

    statistics = {
        "adw_data": aggregate_segment_statistics(related_aw_statistics_model, all_ids),
        "items_count": items_count,
        "top_three_items": top_three_items,
        **aggregations
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
