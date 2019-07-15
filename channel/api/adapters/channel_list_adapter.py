from es_components.managers.channel import ChannelManager


SORT_KEY = {
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "thirty_days_views": "stats.last_30day_views",
    "subscribers": "stats.subscribers",
    "views_per_video": "stats.views_per_video",
}

ALLOWED_RANGE_AGGREGATION = {
    "subscribers": "stats.subscribers",
    "facebook_likes": "social.facebook_likes",
    "twitter_followers": "social.twitter_followers",
    "instagram_followers": "social.instagram_followers",
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "thirty_days_views": "stats.last_30day_views",
    "views_per_video": "stats.views_per_video",
    "sentiment": "stats.sentiment",
    "engage_rate": "stats.engage_rate",
    "age_group_13_17": "stats.age_group_13_17",
    "age_group_18_24": "stats.age_group_18_24",
    "age_group_25_34": "stats.age_group_25_34",
    "age_group_35_44": "stats.age_group_35_44",
    "age_group_45_54": "stats.age_group_45_54",
    "age_group_55_64": "stats.age_group_55_64",
    "age_group_65_": "stats.age_group_65_",
    "gender_male": "stats.gender_male",
    "gender_female": "stats.gender_female",
    "gender_other": "stats.gender_other",
    "video_view_rate": "ads_stats.video_view_rate",
    "ctr": "ads_stats.ctr",
    "ctr_v": "ads_stats.ctr_v",
    "average_cpv": "ads_stats.average_cpv",

}

ALLOWED_AGGREGATIONS = (
    ("country", ("general_data.country", "count")),
    ("category", ("general_data.top_category", "count")),
    ("language", ("general_data.top_language", "count")),
    ("has_audience", ("analytics.has_audience", "count")),
    ("verified", ("analytics.verified", "count")),
    ("is_auth", ("analytics.is_auth", "count")),
    ("is_cms", ("analytics.is_cms", "count")),
    ("has_email", ("custom_properties.emails", "count")),
    ("preferred", ("monetization.preferred", "count")),
    ("has_adwords_data", ("ads_stats", "count")),

    ("subscribers", ("stats.subscribers", "range")),
    ("facebook_likes", ("social.facebook_likes", "range")),
    ("twitter_followers", ("social.twitter_followers", "range")),
    ("instagram_followers", ("social.instagram_followers", "range")),
    ("thirty_days_subscribers", ("stats.last_30day_subscribers", "range")),
    ("thirty_days_views", ("stats.last_30day_views", "range")),
    ("views_per_video", ("stats.views_per_video", "range")),
    ("sentiment", ("stats.sentiment", "range")),
    ("engage_rate", ("stats.engage_rate", "range")),
    ("age_group_13_17", ("stats.age_group_13_17", "range")),
    ("age_group_18_24", ("stats.age_group_18_24", "range")),
    ("age_group_25_34", ("stats.age_group_25_34", "range")),
    ("age_group_35_44", ("stats.age_group_35_44", "range")),
    ("age_group_45_54", ("stats.age_group_45_54", "range")),
    ("age_group_55_64", ("stats.age_group_55_64", "range")),
    ("age_group_65_", ("stats.age_group_65_", "range")),
    ("gender_male", ("stats.gender_male", "range")),
    ("gender_female", ("stats.gender_female", "range")),
    ("gender_other", ("stats.gender_other", "range")),

    ("video_view_rate", ("ads_stats.video_view_rate", "range")),
    ("ctr", ("ads_stats.ctr", "range")),
    ("ctr_v", ("ads_stats.ctr_v", "range")),
    ("average_cpv", ("ads_stats.average_cpv", "range")),

    ("cms__title", ("analytics.cms_title", "count")),

    ("subscribers_outlier", ("stats.subscribers", "percentiles")),
    ("facebook_likes_outlier", ("social.facebook_likes", "percentiles")),
    ("twitter_followers_outlier", ("social.twitter_followers", "percentiles")),
    ("instagram_followers_outlier", ("social.instagram_followers", "percentiles")),
    ("thirty_days_subscribers_outlier", ("stats.last_30day_subscribers", "percentiles")),
    ("thirty_days_views_outlier", ("stats.last_30day_views", "percentiles")),
    ("views_per_video_outlier", ("stats.views_per_video", "percentiles")),
    ("video_view_rate_outlier", ("ads_stats.video_view_rate", "percentiles")),
    ("ctr_outlier", ("ads_stats.ctr", "percentiles")),
    ("ctr_v_outlier", ("ads_stats.ctr_v", "percentiles")),
    ("average_cpv_outlier", ("ads_stats.average_cpv", "percentiles")),

    ("features", "feature_count"),
)

TERMS_FILTER = {
    "country": "general_data.country",
    "language": "general_data.top_language",
    "cms_title": "analytics.cms_title",
    "text_search": "general_data.title"

}

RANGE_FILTER = {
    "subscribers_in": "social.instagram_followers",
    "subscribers_tw": "social.twitter_followers",
    "subscribers_fb": "social.facebook_likes",
    "views_per_video": "stats.views_per_video",
    "engage_rate": "stats.engage_rate",
    "sentiment": "stats.sentiment",
    "thirty_days_views": "stats.last_30day_views",
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "subscribers": "stats.subscribers",

}


class AggregationAdapter:
    allowed_aggregations = ()

    def __init__(self, query_params):
        self.query_params = query_params

    def get_aggregations(self):
        aggregation_fields = self.query_params.pop("aggregations", [])

        if aggregation_fields:
            aggregation_fields = aggregation_fields.pop().split(",")

        allowed_aggregations = dict(self.allowed_aggregations)

        aggs = {}
        aggregations = {}

        for aggregation_field in aggregation_fields:
            agg_params = allowed_aggregations.get(aggregation_field)

            if not agg_params:
                continue

            field, agg = agg_params

            if agg == "count":
                aggregations[aggregation_field + ":count"] = {
                    'terms': {
                        'size': 100000,
                        'field': field,
                        'min_doc_count': 1,
                    }
                }
            elif agg == "range":
                aggregations[aggregation_field + ":min"] = {
                    "min": {"field": field},
                }
                aggregations[aggregation_field + ":max"] = {
                    "max": {"field": field},
                }

            elif agg == "avg":
                aggregations[aggregation_field] = {
                    "avg": {"field": field[4:]}
                }
            elif agg == "percentiles":
                aggregations[aggregation_field] = {
                    "percentiles": {
                        "field": field,
                        "percents": [10, 20, 30, 40, 50, 60, 70, 80, 90],
                    }
                }
            elif agg == "feature_count":
                aggregations["preferred:count"] = {
                    'terms': {
                        'size': 100000,
                        'field': "monetization.preferred",
                        'min_doc_count': 1,
                    }
                }
                aggregations["has_adwords_data:count"] = {
                    'terms': {
                        'size': 100000,
                        'field': "has_adwords_data",
                        'min_doc_count': 1,
                    }
                }
                aggs['preferred'] = 'count'
                aggs['has_adwords_data'] = 'count'
            else:
                continue
            aggs[aggregation_field] = agg

        return aggregations, aggs

    def adapt_aggregation_results(self, aggregation_result, aggs):
        if not aggregation_result:
            return None

        aggregation_results = {}
        for field, agg in aggs.items():
            results = []
            if agg == "count":
                section = getattr(aggregation_result, field + ":count", None)
                if not section:
                    continue
                buckets = section.buckets
                for bucket in buckets:
                    results.append((bucket.key, bucket.doc_count))
                aggregation_results[field + ":" + agg] = results
            elif agg == "range":
                section_min = getattr(aggregation_result, field + ":min", None)
                section_max = getattr(aggregation_result, field + ":max", None)

                if section_max and section_min:
                    results = [section_min.value, section_max.value]
                aggregation_results[field + ":" + agg] = results

            elif agg == "avg":
                avg = getattr(aggregation_result, field, None)
                if avg:
                    results = [avg.value]
                aggregation_results[field] = results

            elif agg == "percentiles":
                percentiles = getattr(aggregation_result, field, None)
                if percentiles:
                    results = [percentiles.values.to_dict()]
                aggregation_results[field] = results

        if 'features' in aggs:
            aggs_preferred = getattr(aggregation_results, "preferred:count")
            aggs_has_adwords_data = getattr(aggregation_results, "has_adwords_data:count")

            aggregation_results['features:count'] = [
                ('Google Preferred', aggs_preferred),
                ('Ad Performance Data', aggs_has_adwords_data)
            ]
        return aggregation_results


class Adapter(AggregationAdapter):
    allowed_aggregations = ALLOWED_AGGREGATIONS
    es_manager = ChannelManager
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER

    def get_limits(self, default_page_size=0):
        size = int(self.query_params.pop("size", [default_page_size]).pop())
        page = int(self.query_params.pop("page", [1]).pop())
        offset = 0 if page <= 1 else page - 1 * size

        return size, offset, page

    def get_sort_rule(self):
        sort_params = self.query_params.pop("sort", None)

        if sort_params:
            key, direction = sort_params[0].split(":")
            field = SORT_KEY.get(key)

            if field:
                return [{field: {"order": direction}}]

    def get_queries(self):
        queries = []
        category = self.query_params.pop("top_category", [None])[0]
        if category is not None:
            regexp = "|".join([".*" + c + ".*" for c in category.split(",")])

            queries.append(
                self.es_manager.query_regexp("general_data.top_category", regexp)
            )
        return [query for query in queries if query is not None]

    def get_filter_range(self):
        filters = []

        for filter_name, es_field_name in self.range_filter.items():

            min, max = self.query_params.pop(filter_name, [None, None])

            if min and max:
                filters.append(self.es_manager.filter_range(es_field_name, gte=min, lte=max))

        return filters

    def get_filters_term(self):
        filters = []

        for filter_name, es_field_name in self.terms_filter.items():

            value = self.query_params.pop(filter_name, [None])[0]
            if value:
                filters.append(self.es_manager.filter_term(es_field_name, value))

        return filters

    def get_filters(self):
        filters_term = self.get_filters_term()
        filters_range = self.get_filter_range()

        return filters_term + filters_range
