from elasticsearch_dsl import Q

from audit_tool.models import AuditCategory
from es_components.constants import Sections
from es_components.countries import COUNTRY_CODES
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder


# pylint: disable=too-many-instance-attributes
class SegmentQueryBuilder:
    MAX_RETRIES = 1000
    MAX_SIZE = 10000
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.ADS_STATS,
                Sections.TASK_US_DATA)

    AD_STATS_RANGE_FIELDS = ("video_view_rate", "average_cpv", "average_cpm", "ctr", "ctr_v", "video_quartile_100_rate")
    STATS_RANGE_FIELDS = ("last_30day_views",)

    def __init__(self, data, with_forced_filters=True):
        """
        :param data: dict -> Query options
        :param video_ids: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.with_forced_filters = with_forced_filters
        self._params = self._map_params(data)

        self.es_manager = VideoManager(sections=self.SECTIONS) if data.get("segment_type") in {0, "video" } else ChannelManager(
            sections=self.SECTIONS)
        self.query_body = self._construct_query()
        self.query_params = self._get_query_params()

    def _map_params(self, params):
        _params = params.copy()
        _params["sentiment"] = self._map_sentiment(_params.pop("sentiment", None))
        _params["score_threshold"] = self._map_score_threshold(_params.pop("score_threshold", None))
        return _params

    def _map_published_at(self, segment_type):
        """
        :return: dict
        """
        if segment_type == 0:
            published_at = "general_data.youtube_published_at"
        else:
            published_at = "stats.last_video_published_at"
        return published_at

    def execute(self, limit=5):
        results = self.es_manager.search(self.query_body, limit=limit).extra(track_total_hits=True).execute()
        return results

    def _get_query_params(self):
        """ Get params used to construct query """
        return self._params

    # pylint: disable=too-many-branches,too-many-statements
    def _construct_query(self):
        """
        Construct Elasticsearch query for segment items
        :param config: dict
        :return: dict
        """
        must_queries = []
        segment_type = self._params.get("segment_type")

        if self._params.get("minimum_views"):
            min_views_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_views",
                flag_name="minimum_views_include_na",
                field_name="stats.views"
            )
            must_queries.append(min_views_ct_queries)

        if segment_type == 1 and self._params.get("minimum_subscribers"):
            min_subs_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_subscribers",
                flag_name="minimum_subscribers_include_na",
                field_name="stats.subscribers"
            )
            must_queries.append(min_subs_ct_queries)

        if segment_type == 1 and self._params.get("minimum_videos"):
            min_vid_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_videos",
                flag_name="minimum_videos_include_na",
                field_name="stats.total_videos_count"
            )
            must_queries.append(min_vid_ct_queries)

        if self._params.get("video_ids"):
            must_queries.append(QueryBuilder().build().must().terms().field("main.id")
                                .value(self._params["video_ids"]).get())

        if self._params.get("last_upload_date"):
            published_at_field = self._map_published_at(segment_type)
            must_queries.append(QueryBuilder().build().must().range()
                                .field(published_at_field).gte(self._params["last_upload_date"]).get())

        if self._params.get("sentiment"):
            must_queries.append(
                QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment")
                    .gte(self._params["sentiment"]).get()
            )

        if self._params.get("gender") is not None:
            must_queries.append(
                QueryBuilder().build().must().term().field("task_us_data.gender").value(self._params["gender"]).get())

        if self._params.get("languages"):
            lang_code_field = "lang_code" if segment_type == 0 else "top_lang_code"
            lang_queries = Q("bool")
            for lang in self._params["languages"]:
                lang_queries |= QueryBuilder().build().should().term().field(f"general_data.{lang_code_field}").value(
                    lang).get()
            must_queries.append(lang_queries)

        if self._params.get("content_categories"):
            content_queries = Q("bool")
            for category in self._params["content_categories"]:
                content_queries |= QueryBuilder().build().should().term().field("general_data.iab_categories").value(
                    category).get()
            must_queries.append(content_queries)

        if self._params.get("exclude_content_categories"):
            content_exclusion_queries = self._get_terms_query(self._params["exclude_content_categories"],
                                                              "general_data.iab_categories", "must_not")
            must_queries.append(content_exclusion_queries)

        if self._params.get("countries"):
            country_queries = Q("bool")
            if self._params.get("countries_include_na"):
                country_queries |= QueryBuilder().build().must_not().exists().field("general_data.country_code").get()
            for country in self._params["countries"]:
                country_code = COUNTRY_CODES.get(country)
                country_queries |= QueryBuilder().build().should().term().field("general_data.country_code").value(
                    country_code).get()
            must_queries.append(country_queries)

        if self._params.get("age_groups"):
            age_queries = Q("bool")
            if self._params.get("age_groups_include_na"):
                age_queries |= QueryBuilder().build().must_not().exists().field("task_us_data.age_group").get()
            for age_group_id in self._params["age_groups"]:
                age_queries |= QueryBuilder().build().should().term().field("task_us_data.age_group").value(
                    age_group_id).get()
            must_queries.append(age_queries)

        if self._params.get("severity_filters"):
            severity_queries = Q("bool")
            for category, scores in self._params["severity_filters"].items():
                for score in scores:
                    # Querying for categories with at least one unique word of target severity score
                    severity_queries &= QueryBuilder().build().must().range().field(
                        f"brand_safety.categories.{category}.severity_counts.{score}").lte(0).get()
            must_queries.append(severity_queries)

        if self._params.get("score_threshold") is not None:
            score_threshold = self._params["score_threshold"]
            score_queries = Q("bool")
            if score_threshold == 0:
                score_queries |= QueryBuilder().build().must_not().exists().field("brand_safety.overall_score").get()
            score_queries |= QueryBuilder().build().must().range().field("brand_safety.overall_score").gte(
                score_threshold).get()
            must_queries.append(score_queries)

            if self._params.get("brand_safety_categories"):
                safety_queries = Q("bool")
                for category in self._params["brand_safety_categories"]:
                    safety_queries &= QueryBuilder().build().must().range().field(
                        f"brand_safety.categories.{category}.category_score").gte(score_threshold).get()
                must_queries.append(safety_queries)

        if self._params.get("is_vetted") is not None:
            vetted_query = QueryBuilder().build().must().exists().field("task_us_data").get() \
                if self._params["is_vetted"] \
                else QueryBuilder().build().must_not().exists().field("task_us_data").get()
            must_queries.append(vetted_query)

        if self._params.get("vetted_after"):
            vetted_after_query = QueryBuilder().build().must().range() \
                .field(f"{Sections.TASK_US_DATA}.last_vetted_at") \
                .gte(self._params["vetted_after"]).get()
            must_queries.append(vetted_after_query)

        if self._params.get("mismatched_language") is not None:
            mismatched_language_queries = QueryBuilder().build().must().term().field(
                "task_us_data.mismatched_language").value(self._params["mismatched_language"]).get()
            mismatched_language_queries |= QueryBuilder().build().must_not().exists().field(
                "task_us_data.mismatched_language").get()
            must_queries.append(mismatched_language_queries)

        if self._params.get("content_type") is not None:
            content_type_query = QueryBuilder().build().must()\
                .term().field(f"{Sections.TASK_US_DATA}.content_type").value(self._params["content_type"]).get()
            must_queries.append(content_type_query)

        if self._params.get("content_quality") is not None:
            content_quality_query = QueryBuilder().build().must() \
                .term().field(f"{Sections.TASK_US_DATA}.content_quality").value(self._params["content_quality"]).get()
            must_queries.append(content_quality_query)

        ads_stats_queries = self._get_ads_stats_queries()
        must_queries.append(ads_stats_queries)

        if self._params.get("last_30day_views"):
            query = self._get_range_queries(["last_30day_views"], Sections.STATS)
            if self._params.get("ads_stats_include_na") is True:
                query |= QueryBuilder().build().must_not().exists().field(f"{Sections.STATS}.last_30day_views").get()
            must_queries.append(query)

        query = Q("bool", must=must_queries)

        if self.with_forced_filters is True:
            forced_filters = self.es_manager.forced_filters()
            query &= forced_filters

        return query

    # pylint: enable=too-many-branches,too-many-statements

    def get_numeric_include_na_queries(self, attr_name: str, flag_name: str, field_name: str):
        """
        get the combined queries for a gte field that supports the "include n/a" option
        :param attr_name: str, name of the attribute for this class
        :param flag_name: str, name of the include n/a flag attribute
        :param field_name: str, name of the dot-notated field in ES
        :return Q: the constructed Q query
        """
        queries = Q("bool")
        if self._params.get(flag_name):
            if flag_name == "minimum_subscribers_include_na":
                queries |= QueryBuilder().build().should().term().field("stats.hidden_subscriber_count").value(
                    True).get()
            else:
                queries |= QueryBuilder().build().should().term().field(field_name).value(0).get()
        queries |= QueryBuilder().build().should().range().field(field_name) \
            .gte(self._params.get(attr_name)).get()
        return queries

    def _get_ads_stats_queries(self):
        queries = self._get_range_queries(self.AD_STATS_RANGE_FIELDS, Sections.ADS_STATS)
        if self._params.get("ads_stats_include_na") is True:
            queries |= QueryBuilder().build().must_not().exists().field(Sections.ADS_STATS).get()
        return queries

    def _get_range_queries(self, fields, section):
        query = Q("bool")
        for key in fields:
            field = f"{section}.{key}"
            try:
                params = str(self._params[key])
                lower_bound, upper_bound = params.replace(" ", "").split(",")
            except (KeyError, AttributeError, ValueError):
                pass
            else:
                if lower_bound and upper_bound and float(lower_bound) > float(upper_bound):
                    raise ValueError(f"Lower bound must be less than upper bound. {key}: {params}")
                if lower_bound:
                    query &= QueryBuilder().build().must().range().field(field).gte(lower_bound).get()
                if upper_bound:
                    query &= QueryBuilder().build().must().range().field(field).lte(upper_bound).get()
        return query

    def _get_terms_query(self, terms, field, operator="must"):
        terms_query = getattr(QueryBuilder().build(), operator)().terms().field(field).value(terms).get()
        return terms_query

    @staticmethod
    def map_content_categories(content_category_ids: list):
        mapping = AuditCategory.get_all(iab=True, unique=True)
        to_string = [mapping[str(_id)] for _id in content_category_ids] or []
        return to_string

    def _map_sentiment(self, sentiment: int):
        if sentiment == 1:
            threshold = 0
        elif sentiment == 2:
            threshold = 79
        elif sentiment == 3:
            threshold = 90
        elif sentiment == 4:
            threshold = 100
        else:
            threshold = None
        return threshold

    def _map_score_threshold(self, score_threshold: int):
        """
        Map blacklist severity from client to score
        :param score_threshold: int
        :return: int
        """
        if score_threshold == 1:
            threshold = 0
        elif score_threshold == 2:
            threshold = 70
        elif score_threshold == 3:
            threshold = 80
        elif score_threshold == 4:
            threshold = 90
        else:
            threshold = None
        return threshold
# pylint: enable=too-many-instance-attributes
