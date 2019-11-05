from elasticsearch_dsl import Q

import brand_safety.constants as constants
from audit_tool.models import AuditCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager


class BrandSafetyQueryBuilder(object):
    MAX_RETRIES = 1000
    MAX_SIZE = 10000
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

    def __init__(self, data, overall_score: int = None, video_ids: list = None):
        """
        :param data: dict -> Query options
        :param overall_score: int -> Overall score threshold (gte for whitelist, lte for blacklist)
        :param related_to: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.overall_score = overall_score
        self.video_ids = video_ids
        self.list_type = data["list_type"]
        self.segment_type = data["segment_type"]
        # Score threshold for brand safety categories
        self.score_threshold = data.get("score_threshold", 0)
        # For blacklists, FE will send 1,2, or 3 for score threshold. Should be mapped from 1-100 scale
        self.score_threshold = self.score_threshold if self.list_type == "whitelist" else self._map_blacklist_severity(self.score_threshold)
        self.languages = data.get("languages", [])
        self.minimum_option = data.get("minimum_option", 0)
        self.youtube_categories = data.get("youtube_categories", [])
        self.brand_safety_categories = data.get("brand_safety_categories", [])
        self.options = self._get_segment_options()

        self.es_manager = ChannelManager(sections=self.SECTIONS) if self.segment_type == constants.CHANNEL else VideoManager(sections=self.SECTIONS)
        self.query_body = self._construct_query()

    def execute(self, limit=5):
        query = Q(self.query_body)
        results = self.es_manager.search(query, limit=limit).execute()
        return results

    def _get_segment_options(self) -> dict:
        """
        Get options for segment wizard
        :param segment_type: (str) channel, video
        :param list_type: (str) whitelist, blacklist
        :return: dict
        """
        options = {
            "channel": {
                "index": constants.CHANNELS_INDEX,
                "minimum_option": "stats.subscribers",
                "youtube_category_field": "general_data.top_category"
            },
            "video": {
                "index": constants.VIDEOS_INDEX,
                "minimum_option": "stats.views",
                "youtube_category_field": "general_data.category"
            },
            "range_param": {
                constants.BLACKLIST: "lte",
                constants.WHITELIST: "gte"
            },
        }
        config = {
            "index": options[self.segment_type]["index"],
            "minimum_option": options[self.segment_type]["minimum_option"],
            "range_param": options["range_param"][self.list_type],
            "youtube_category_field": options[self.segment_type]["youtube_category_field"]
        }
        return config

    def _construct_query(self) -> dict:
        """
        Construct Elasticsearch query for segment items
        :param config: dict
        :return: dict
        """
        query_body = {
            "bool": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                # Minimum option (views | subscribers)
                                "range": {

                                }
                            },
                            {
                                "bool": {
                                    # language
                                    "should": []
                                }
                            },
                            {
                                "bool": {
                                    # youtube_category
                                    "should": []
                                }
                            },
                            {
                                # brand safety categories
                                "bool": {
                                    "filter": []
                                }
                            },
                            {
                                "exists": {
                                    "field": "brand_safety"
                                }
                            }
                        ]
                    }
                }
            }
        }
        must_statements = query_body["bool"]["filter"]["bool"]["must"]
        if self.overall_score:
            # Get items with overall score <= or >= self.overall score depending on self.segment_type
            threshold_operator = "gte" if self.list_type == constants.WHITELIST else "lte"
            overall_score_threshold = {
                "range": {"brand_safety.overall_score": {threshold_operator: self.overall_score}}
            }
            must_statements.append(overall_score_threshold)

        if self.video_ids:
            related_to = {
                "terms": {
                    "_id": self.video_ids
                }
            }
            must_statements.append(related_to)
        # Set refs for easier access
        minimum_option = must_statements[0]["range"]
        language_filters = must_statements[1]["bool"]["should"]
        youtube_categories_filters = must_statements[2]["bool"]["should"]
        category_score_filters = must_statements[3]["bool"]["filter"]

        # e.g. {"range": {"categories.1.category_score": {"gte": 50}}}
        category_score_filter_params = [
            {"range": {"brand_safety.categories.{}.category_score".format(category): {self.options["range_param"]: self.score_threshold}}}
            for category in self.brand_safety_categories
        ]
        language_filter_params = [
            {"term": {"brand_safety.language": language}}
            for language in self.languages
        ]
        youtube_category_filter_params = [
            {"term": {self.options["youtube_category_field"]: category}}
            for category in self.youtube_categories
        ]

        # Add filters to refs
        category_score_filters.extend(category_score_filter_params)
        language_filters.extend(language_filter_params)
        youtube_categories_filters.extend(youtube_category_filter_params)

        # Sets range query in must clause
        # e.g. { "range": { "subscribers": { "gte": 1000 } }
        minimum_option[self.options["minimum_option"]] = {"gte": self.minimum_option}
        return query_body

    def _map_blacklist_severity(self, score_threshold: int):
        """
        Map blacklist severity from client to score
        :param score_threshold: int
        :return: int
        """
        if score_threshold == 1:
            threshold = 69
        elif score_threshold == 2:
            threshold = 79
        elif score_threshold == 3:
            threshold = 89
        else:
            threshold = 100
        return threshold

    @staticmethod
    def map_youtube_categories(youtube_category_ids):
        mapping = {
            _id: category for _id, category in AuditCategory.get_all().items()
        }
        to_string = [mapping[str(_id)] for _id in youtube_category_ids]
        return to_string
