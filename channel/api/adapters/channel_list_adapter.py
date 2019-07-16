from es_components.managers.channel import ChannelManager
from es_components.query_builder import QueryBuilder

AGGREGATION_COUNT_SIZE = 100000
AGGREGATION_PERCENTS = tuple(range(10, 100, 10))
DEFAULT_PAGE_NUMBER = 1
DEFAULT_PAGE_SIZE = 50

SORT_KEY = {
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "thirty_days_views": "stats.last_30day_views",
    "subscribers": "stats.subscribers",
    "views_per_video": "stats.views_per_video",
}

RANGE_AGGREGATION = {
    "subscribers": "stats.subscribers",
    "facebook_likes": "social.facebook_likes",
    "twitter_followers": "social.twitter_followers",
    "instagram_followers": "social.instagram_followers",
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "thirty_days_views": "stats.last_30day_views",
    "views_per_video": "stats.views_per_video",
    "sentiment": "stats.sentiment",
    "engage_rate": "stats.engage_rate",
    "age_group_13_17": "analytics.age_group_13_17",
    "age_group_18_24": "analytics.age_group_18_24",
    "age_group_25_34": "analytics.age_group_25_34",
    "age_group_35_44": "analytics.age_group_35_44",
    "age_group_45_54": "analytics.age_group_45_54",
    "age_group_55_64": "analytics.age_group_55_64",
    "age_group_65_": "analytics.age_group_65_",
    "gender_male": "analytics.gender_male",
    "gender_female": "analytics.gender_female",
    "gender_other": "analytics.gender_other",
    "video_view_rate": "ads_stats.video_view_rate",
    "ctr": "ads_stats.ctr",
    "ctr_v": "ads_stats.ctr_v",
    "average_cpv": "ads_stats.average_cpv",
}

COUNT_AGGREGATION = {
    "country:count": "general_data.country",
    "category:count": "general_data.top_category",
    "language:count": "general_data.top_language",
    "has_audience:count": "analytics",
    "verified:count": "analytics.verified",
    "is_auth:count": "analytics.is_auth",
    "is_cms:count": "analytics.is_cms",
    "has_email:count": "custom_properties.emails",
    "preferred:count": "monetization.preferred",
    "has_adwords_data:count": "ads_stats",
    "cms__title:count": "analytics.cms_title"
}

FEATURE_COUNT_AGGREGATION = {
    "Google Preferred": "monetization.preferred",
    "Ad Performance Data": "ads_stats",
}

PERCENTILES_AGGREGATION = {
    "subscribers_outlier": "stats.subscribers",
    "facebook_likes_outlier": "social.facebook_likes",
    "twitter_followers_outlier": "social.twitter_followers",
    "instagram_followers_outlier": "social.instagram_followers",
    "thirty_days_subscribers_outlier": "stats.last_30day_subscribers",
    "thirty_days_views_outlier": "stats.last_30day_views",
    "views_per_video_outlier": "stats.views_per_video",
    "video_view_rate_outlier": "ads_stats.video_view_rate",
    "ctr_outlier": "ads_stats.ctr",
    "ctr_v_outlier": "ads_stats.ctr_v",
    "average_cpv_outlier": "ads_stats.average_cpv",
}

TERMS_FILTER = {
    "country": "general_data.country",
    "language": "general_data.top_language",
    "cms_title": "analytics.cms_title",
    "text_search": "general_data.title",
    "category": "general_data.top_category"
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
    range_aggregation = RANGE_AGGREGATION
    count_aggregation = COUNT_AGGREGATION
    feature_count_aggregation = FEATURE_COUNT_AGGREGATION
    percentiles_aggregation = PERCENTILES_AGGREGATION

    def get_range_aggs(self):
        range_aggs = {}

        for agg_name, field in self.range_aggregation.items():
            range_aggs["{}:min".format(agg_name)] = {
                "min": {"field": field},
            }
            range_aggs["{}:max".format(agg_name)] = {
                "max": {"field": field},
            }
        return range_aggs

    def get_count_aggs(self):
        count_aggs = {}

        count_aggregation = self.count_aggregation
        count_aggregation.update(self.feature_count_aggregation)

        for agg_name, field in count_aggregation.items():
            count_aggs[agg_name] = {
                "terms": {
                    "size": AGGREGATION_COUNT_SIZE,
                    "field": field,
                    "min_doc_count": 1,
                }
            }
        return count_aggs

    def get_percentiles_aggs(self):
        percentiles_aggs = {}

        for agg_name, field in self.percentiles_aggregation.items():
            percentiles_aggs[agg_name] = {
                "percentiles": {
                    "field": field,
                    "percents": AGGREGATION_PERCENTS,
                }
            }
        return percentiles_aggs

    def get_aggregations(self):
        aggregation = {}

        aggregation.update(self.get_range_aggs())
        aggregation.update(self.get_count_aggs())
        aggregation.update(self.get_percentiles_aggs())

        return aggregation

    def adapt_range_aggs_result(self, aggregation_result):
        range_aggs_result = {}

        for agg_name, field in self.range_aggregation.items():
            results = []
            min_field = "{}:min".format(agg_name)
            max_field = "{}:max".format(agg_name)

            section_min = getattr(aggregation_result, min_field)
            section_max = getattr(aggregation_result, max_field)

            if section_max and section_min:
                results = [section_min.value, section_max.value]

            range_aggs_result["{}:range".format(agg_name)] = results

        return range_aggs_result

    def adapt_count_aggs_result(self, aggregation_result):

        count_aggs_result = {}

        for agg_name, field in self.count_aggregation.items():
            results = []
            section = getattr(aggregation_result, agg_name)

            buckets = section.buckets
            for bucket in buckets:
                results.append((bucket.key, bucket.doc_count))

            count_aggs_result[agg_name] = results

        return count_aggs_result

    def adapt_percentiles_aggs_result(self, aggregation_result):
        percentiles_aggs_result = {}

        for agg_name, field in self.percentiles_aggregation.items():
            percentiles = getattr(aggregation_result, agg_name)
            results = [percentiles.values.to_dict()]
            percentiles_aggs_result[agg_name] = results

        return percentiles_aggs_result

    def adapt_features_aggs_result(self, aggregation_result):
        features_aggs_result = {}

        for agg_name, field in self.feature_count_aggregation.items():
            features_aggs_result[agg_name] = getattr(aggregation_result, agg_name)

        return features_aggs_result

    def adapt_aggregation_results(self, aggregation_result):
        result = {}

        if aggregation_result:

            result.update(self.adapt_count_aggs_result(aggregation_result))
            result.update(self.adapt_range_aggs_result(aggregation_result))
            result.update(self.adapt_percentiles_aggs_result(aggregation_result))
            result.update(self.adapt_features_aggs_result(aggregation_result))

        return result


class Adapter(AggregationAdapter):
    es_manager = ChannelManager
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER

    def __init__(self, query_params):
        self.query_params = query_params

    def get_aggregations(self):
        if self.query_params.get("aggregations"):
            return super(Adapter, self).get_aggregations()

    def get_limits(self):
        size = int(self.query_params.get("size", [DEFAULT_PAGE_SIZE])[0])
        page = self.query_params.get("page", [DEFAULT_PAGE_NUMBER])
        if len(page) > 1:
            raise ValueError("Passed more than one page number")

        page = int(page[0])
        offset = 0 if page <= 1 else (page - 1) * size

        return size, offset, page

    def get_sort_rule(self):
        sort_params = self.query_params.get("sort", None)

        if sort_params:
            key, direction = sort_params.split(":")
            field = SORT_KEY.get(key)

            if field:
                return [{field: {"order": direction}}]

    def get_filter_range(self):
        filters = []

        for filter_name, es_field_name in self.range_filter.items():

            min, max = self.query_params.get(filter_name, [None, None])

            if min or max:
                filters.append(
                    QueryBuilder().create().must().range().field(es_field_name).gte(min).lte(max).get()
                )

        return filters

    def get_filters_term(self):
        filters = []

        for filter_name, es_field_name in self.terms_filter.items():

            value = self.query_params.get(filter_name, [None])[0]
            if value:
                filters.append(
                    QueryBuilder().create().must().term().field(es_field_name).value(value).get()
                )

        return filters

    def get_filters(self):
        filters_term = self.get_filters_term()
        filters_range = self.get_filter_range()

        return filters_term + filters_range
