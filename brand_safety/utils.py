from audit_tool.models import AuditCategory
from elasticsearch_dsl import Q
from es_components.constants import Sections
from es_components.countries import COUNTRY_CODES
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
import brand_safety.constants as constants


class BrandSafetyQueryBuilder(object):
    MAX_RETRIES = 1000
    MAX_SIZE = 10000
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

    def __init__(self, data, video_ids: list = None, with_forced_filters=True):
        """
        :param data: dict -> Query options
        :param video_ids: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.with_forced_filters = with_forced_filters
        self.video_ids = video_ids
        self.list_type = data.get("list_type", "whitelist")
        self.segment_type = int(data["segment_type"])
        # Score threshold for brand safety categories
        self.original_score_threshold = data.get("score_threshold")
        self.score_threshold = self._map_score_threshold(data.get("score_threshold", 0))
        self.sentiment = self._map_sentiment(data.get("sentiment", 0))
        self.last_upload_date = data.get("last_upload_date")
        self.minimum_views = data.get("minimum_views", 0)
        self.minimum_views_include_na = data.get("minimum_views_include_na", None)
        self.minimum_subscribers = data.get("minimum_subscribers", 0)
        self.minimum_subscribers_include_na = data.get("minimum_subscribers_include_na", None)
        self.minimum_videos = data.get("minimum_videos", 0)
        self.minimum_videos_include_na = data.get("minimum_videos_include_na", None)

        self.content_categories = data.get("content_categories", [])
        self.countries = data.get("countries", [])
        self.countries_include_na = data.get("countries_include_na", None)
        self.languages = data.get("languages", [])
        self.severity_filters = data.get("severity_filters", {})
        self.brand_safety_categories = data.get("brand_safety_categories", [])
        self.age_groups = data.get("age_groups", [])
        self.age_groups_include_na = data.get("age_groups_include_na", None)
        self.gender = data.get("gender", None)
        self.is_vetted = data.get("is_vetted", None)
        self.vetted_after = data.get("vetted_after", None)
        self.mismatched_language = data.get("mismatched_language", None)

        self.options = self._get_segment_options()
        self.es_manager = VideoManager(sections=self.SECTIONS) if self.segment_type == 0 else ChannelManager(sections=self.SECTIONS)
        self.query_body = self._construct_query()
        self.query_params = self._get_query_params()

    def execute(self, limit=5):
        results = self.es_manager.search(self.query_body, limit=limit).extra(track_total_hits=True).execute()
        return results

    def _get_query_params(self):
        query_params = {
            "severity_filters": self.severity_filters,
            "score_threshold": self.original_score_threshold,
            "content_categories": self.content_categories,
            "languages": self.languages,
            "countries": self.countries,
            "countries_include_na": self.countries_include_na,
            "sentiment": self.sentiment,
            "minimum_views": self.minimum_views,
            "minimum_views_include_na": self.minimum_views_include_na,
            "minimum_subscribers": self.minimum_subscribers,
            "minimum_subscribers_include_na": self.minimum_subscribers_include_na,
            "minimum_videos": self.minimum_videos,
            "minimum_videos_include_na": self.minimum_videos_include_na,
            "last_upload_date": self.last_upload_date,
            "age_groups": self.age_groups,
            "age_groups_include_na": self.age_groups_include_na,
            "gender": self.gender,
            "is_vetted": self.is_vetted,
            "mismatched_language": self.mismatched_language,
            "vetted_after": self.vetted_after,
        }
        return query_params

    def _get_segment_options(self) -> dict:
        """
        Get options for segment wizard
        :return: dict
        """
        score_range_options = {
            constants.BLACKLIST: "lte",
            constants.WHITELIST: "gte"
        }
        options = {
            0: {
                "index": constants.VIDEOS_INDEX,
                "published_at": "general_data.youtube_published_at",
                "range_param": score_range_options[self.list_type],
            },
            1: {
                "index": constants.CHANNELS_INDEX,
                "published_at": "stats.last_video_published_at",
                "range_param": score_range_options[self.list_type],
            },
        }
        return options[self.segment_type]

    def _construct_query(self):
        """
        Construct Elasticsearch query for segment items
        :param config: dict
        :return: dict
        """
        must_queries = []

        if self.minimum_views:
            min_views_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_views",
                flag_name="minimum_views_include_na",
                field_name="stats.views"
            )
            must_queries.append(min_views_ct_queries)

        if self.segment_type == 1 and self.minimum_subscribers:
            min_subs_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_subscribers",
                flag_name="minimum_subscribers_include_na",
                field_name="stats.subscribers"
            )
            must_queries.append(min_subs_ct_queries)

        if self.segment_type == 1 and self.minimum_videos:
            min_vid_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_videos",
                flag_name="minimum_videos_include_na",
                field_name="stats.total_videos_count"
            )
            must_queries.append(min_vid_ct_queries)

        if self.video_ids:
            must_queries.append(QueryBuilder().build().must().terms().field("main.id").value(self.video_ids).get())

        if self.last_upload_date:
            must_queries.append(QueryBuilder().build().must().range().field(f"{self.options['published_at']}").gte(self.last_upload_date).get())

        if self.sentiment:
            must_queries.append(QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.sentiment).get())

        if self.gender is not None:
            must_queries.append(QueryBuilder().build().must().term().field("task_us_data.gender").value(self.gender).get())

        if self.languages:
            lang_code_field = "lang_code" if self.segment_type == 0 else "top_lang_code"
            lang_queries = Q("bool")
            for lang in self.languages:
                lang_queries |= QueryBuilder().build().should().term().field(f"general_data.{lang_code_field}").value(lang).get()
            must_queries.append(lang_queries)

        if self.content_categories:
            content_queries = Q("bool")
            for category in self.content_categories:
                content_queries |= QueryBuilder().build().should().term().field("general_data.iab_categories").value(category).get()
            must_queries.append(content_queries)

        if self.countries:
            country_queries = Q("bool")
            if self.countries_include_na:
                country_queries |= QueryBuilder().build().must_not().exists().field("general_data.country_code").get()
            for country in self.countries:
                country_code = COUNTRY_CODES.get(country)
                country_queries |= QueryBuilder().build().should().term().field("general_data.country_code").value(country_code).get()
            must_queries.append(country_queries)

        if self.age_groups:
            age_queries = Q("bool")
            if self.age_groups_include_na:
                age_queries |= QueryBuilder().build().must_not().exists().field("task_us_data.age_group").get()
            for age_group_id in self.age_groups:
                age_queries |= QueryBuilder().build().should().term().field("task_us_data.age_group").value(age_group_id).get()
            must_queries.append(age_queries)

        if self.severity_filters:
            severity_queries = Q("bool")
            for category, scores in self.severity_filters.items():
                for score in scores:
                    # Querying for categories with at least one unique word of target severity score
                    severity_queries &= QueryBuilder().build().must().range().field(f"brand_safety.categories.{category}.severity_counts.{score}").lte(0).get()
            must_queries.append(severity_queries)

        if self.score_threshold is not None:
            overall_score_query = QueryBuilder().build().must().range().field("brand_safety.overall_score").gt(self.score_threshold).get()
            must_queries.append(overall_score_query)

            if self.brand_safety_categories:
                safety_queries = Q("bool")
                for category in self.brand_safety_categories:
                    safety_queries &= QueryBuilder().build().must().range().field(f"brand_safety.categories.{category}.category_score").gte(self.score_threshold).get()
                must_queries.append(safety_queries)

        if self.is_vetted is not None:
            vetted_query = QueryBuilder().build().must().exists().field("task_us_data").get() \
                if self.is_vetted \
                else QueryBuilder().build().must_not().exists().field("task_us_data").get()
            must_queries.append(vetted_query)

        if self.vetted_after:
            vetted_after_query = QueryBuilder().build().must().range().field(f"{Sections.TASK_US_DATA}.last_vetted_at")\
                .gte(self.vetted_after).get()
            must_queries.append(vetted_after_query)

        if self.mismatched_language is not None:
            mismatched_language_queries = QueryBuilder().build().must().term().field(
                "task_us_data.mismatched_language").value(self.mismatched_language).get()
            mismatched_language_queries |= QueryBuilder().build().must_not().exists().field(
                "task_us_data.mismatched_language").get()
            must_queries.append(mismatched_language_queries)

        query = Q("bool", must=must_queries)

        if self.with_forced_filters is True:
            forced_filters = self.es_manager.forced_filters()
            query &= forced_filters

        return query

    def get_numeric_include_na_queries(self, attr_name: str, flag_name: str, field_name: str):
        """
        get the combined queries for a gte field that supports the "include n/a" option
        :param attr_name: str, name of the attribute for this class
        :param flag_name: str, name of the include n/a flag attribute
        :param field_name: str, name of the dot-notated field in ES
        :return Q: the constructed Q query
        """
        queries = Q("bool")
        if getattr(self, flag_name):
            if flag_name == "minimum_subscribers_include_na":
                queries |= QueryBuilder().build().should().term().field("stats.hidden_subscriber_count").value(True).get()
            else:
                queries |= QueryBuilder().build().should().term().field(field_name).value(0).get()
        queries |= QueryBuilder().build().should().range().field(field_name) \
            .gte(getattr(self, attr_name)).get()
        return queries

    def _map_blacklist_severity(self, score_threshold: int):
        """
        Map blacklist severity from client to score
        :param score_threshold: int
        :return: int
        """
        if score_threshold == 1:
            threshold = 0
        elif score_threshold == 2:
            threshold = 79
        elif score_threshold == 3:
            threshold = 89
        else:
            threshold = 100
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
            threshold = 69
        elif score_threshold == 3:
            threshold = 79
        elif score_threshold == 4:
            threshold = 89
        else:
            threshold = None
        return threshold

    @staticmethod
    def map_content_categories(content_category_ids: list):
        mapping = {
            _id: category for _id, category in AuditCategory.get_all(iab=True, unique=True).items()
        }
        to_string = [mapping[str(_id)] for _id in content_category_ids] or []
        return to_string

    def _map_sentiment(self, sentiment: int):
        if sentiment == 1:
            threshold = 0
        elif sentiment == 2:
            threshold = 50
        elif sentiment == 3:
            threshold = 70
        elif sentiment == 4:
            threshold = 90
        else:
            threshold = None
        return threshold
