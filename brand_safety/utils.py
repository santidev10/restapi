from elasticsearch_dsl import Q

import brand_safety.constants as constants
from audit_tool.models import AuditCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder


class BrandSafetyQueryBuilder(object):
    MAX_RETRIES = 1000
    MAX_SIZE = 10000
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

    def __init__(self, data, overall_score: int = None, video_ids: list = None):
        """
        :param data: dict -> Query options
        :param overall_score: int -> Overall score threshold (gte for whitelist, lte for blacklist)
        :param video_ids: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.overall_score = overall_score
        self.video_ids = video_ids
        self.list_type = data.get("list_type", "whitelist")
        self.segment_type = int(data["segment_type"])
        # Score threshold for brand safety categories
        self.score_threshold = self._map_score_threshold(data.get("score_threshold", 0))
        self.sentiment = self._map_sentiment(data.get("sentiment", 0))
        self.last_upload_date = data.get("last_upload_date")
        self.minimum_views = data.get("minimum_views")
        self.minimum_subscribers = data.get("minimum_subscribers")

        self.content_categories = data.get("content_categories", [])
        self.countries = data.get("countries", [])
        self.languages = data.get("languages", [])
        self.severity_filters = data.get("severity_filters", {})
        self.brand_safety_categories = data.get("brand_safety_categories", [])

        self.options = self._get_segment_options()
        self.es_manager = VideoManager(sections=self.SECTIONS) if self.segment_type == 0 else ChannelManager(sections=self.SECTIONS)
        self.query_body = self._construct_query()
        self.query_params = self._get_query_params()

    def execute(self, limit=5):
        results = self.es_manager.search(self.query_body, limit=limit).execute()
        return results

    def _get_query_params(self):
        query_params = {
            "languages": self.languages,
            "score_threshold": self.score_threshold,
            "minimum_views": self.minimum_views,
            "minimum_subscribers": self.minimum_subscribers,
            "content_categories": self.content_categories,
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
        should_queries = []

        if self.minimum_views:
            must_queries.append(QueryBuilder().build().must().range().field("stats.views").gte(self.minimum_views).get())

        if self.segment_type == 1 and self.minimum_subscribers:
            must_queries.append(QueryBuilder().build().must().range().field("stats.subscribers").gte(self.minimum_subscribers).get())

        if self.overall_score:
            if self.list_type == constants.WHITELIST:
                must_queries.append(QueryBuilder().build().must().range().field("brand_safety.overall_score").gte(self.minimum_subscribers).get())
            else:
                must_queries.append(QueryBuilder().build().must().range().field("brand_safety.overall_score").lte(self.minimum_subscribers).get())

        if self.video_ids:
            must_queries.append(QueryBuilder().build().must().terms().field("main.id").value(self.video_ids).get())

        if self.last_upload_date:
            must_queries.append(QueryBuilder().build().must().range().field(f"{self.options['published_at']}").gte(
                self.last_upload_date).get())

        if self.sentiment:
            must_queries.append(QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(
                self.sentiment).get())

        for lang in self.languages:
            should_queries.append(QueryBuilder().build().should().term().field("brand_safety.language").value(lang).get())

        for category in self.content_categories:
            should_queries.append(QueryBuilder().build().should().term().field("general_data.iab_categories").value(category).get())

        for country in self.countries:
            should_queries.append(QueryBuilder().build().should().term().field("general_data.country").value(country).get())

        if self.severity_filters:
            for category, scores in self.severity_filters.items():
                for score in scores:
                    # Querying for categories with at least one unique word of target severity score
                    must_queries.append(QueryBuilder().build().must().range().field(f"brand_safety.categories.{category}.severity_counts.{score}").gt(0).get())

        if self.brand_safety_categories and self.score_threshold:
            for category in self.brand_safety_categories:
                must_queries.append(QueryBuilder().build().must().range().field(f"brand_safety.categories.{category}.category_score").gte(self.score_threshold).get())

        query = Q(
            'bool',
            must=must_queries,
            should=should_queries,
        )
        return query

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
            _id: category for _id, category in AuditCategory.get_all(iab=True).items()
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
