from django.conf import settings

from audit_tool.models import AuditCategory
import brand_safety.constants as constants
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException


def get_es_data(item_ids, index_name):
    """
    Encapsulate getting es data to catch ElasticSearchConnectorException
        On ElasticSearchConnectorException, return it to be handled by view
    :param item_ids: str or list
    :param index_name: str
    :return: dict or ElasticSearchConnectorException
    """
    try:
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            item_ids,
            settings.BRAND_SAFETY_TYPE)
        return es_data
    except ElasticSearchConnectorException:
        return ElasticSearchConnectorException


class BrandSafetyQueryBuilder(object):
    MAX_SIZE = 10000

    def __init__(self, data, overall_score: int = None, related_to: str = None):
        """
        :param data: dict -> Query options
        :param overall_score: int -> Overall score threshold (gte for whitelist, lte for blacklist)
        :param related_to: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.overall_score = overall_score
        self.related_to = related_to
        self.list_type = data["list_type"]
        self.segment_type = data["segment_type"]
        self.score_threshold = data.get("score_threshold", 0)
        self.score_threshold = self.score_threshold if self.list_type == "whitelist" else self._map_blacklist_severity(self.score_threshold)
        self.languages = data.get("languages", [])
        self.minimum_option = data.get("minimum_option", 0)
        self.youtube_categories = data.get("youtube_categories", [])
        self.brand_safety_categories = data.get("brand_safety_categories", [])
        self.options = self._get_segment_options()
        self.query_body = self._construct_query()

    def execute(self):
        try:
            result = ElasticSearchConnector(index_name=self.options["index"]).search(
                doc_type=settings.BRAND_SAFETY_TYPE,
                body=self.query_body,
                size=self.MAX_SIZE,
            )
        except ElasticSearchConnectorException:
            raise ElasticSearchConnectorException
        return result

    def _get_segment_options(self) -> dict:
        """
        Get options for segment wizard
        :param segment_type: (str) channel, video
        :param list_type: (str) whitelist, blacklist
        :return: dict
        """
        options = {
            "channel": {
                "index": settings.BRAND_SAFETY_CHANNEL_INDEX,
                "minimum_option": "subscribers"
            },
            "video": {
                "index": settings.BRAND_SAFETY_VIDEO_INDEX,
                "minimum_option": "views"
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
        }
        return config

    def _construct_query(self) -> dict:
        """
        Construct Elasticsearch query for segment items
        :param config: dict
        :return: dict
        """
        query_body = {
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    # Minimum option (views | subscribers)
                                    "range": {}
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
                                    "nested": {
                                        "path": "categories",
                                        "query": {
                                            "bool": {
                                                "filter": []
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        must_statements = query_body["query"]["bool"]["filter"]["bool"]["must"]
        if self.overall_score:
            # Get items with overall score <= or >= self.overall score depending on self.segment_type
            threshold_operator = "gte" if self.list_type == constants.WHITELIST else "lte"
            overall_score_threshold = {
                "range": {"overall_score": {threshold_operator: self.overall_score}}
            }
            must_statements.append(overall_score_threshold)

        if self.related_to:
            # Get items related to self.related_to e.g. Get videos with channel_id=related_to
            related_key = self._get_related_key()
            related_to = {
                "term": {
                    related_key: {"value": self.related_to}
                }
            }
            must_statements.append(related_to)
        # Set refs for easier access
        minimum_option = query_body["query"]["bool"]["filter"]["bool"]["must"][0]["range"]
        language_filters = query_body["query"]["bool"]["filter"]["bool"]["must"][1]["bool"]["should"]
        youtube_categories_filters = query_body["query"]["bool"]["filter"]["bool"]["must"][2]["bool"]["should"]
        category_score_filters = query_body["query"]["bool"]["filter"]["bool"]["must"][3]["nested"]["query"]["bool"]["filter"]

        # e.g. {"range": {"categories.1.category_score": {"gte": 50}}}
        category_score_params = [
            {"range": {"categories.{}.category_score".format(category): {self.options["range_param"]: self.score_threshold}}}
            for category in self.brand_safety_categories
        ]
        youtube_categories = [
            {"term": {"youtube_category": category}}
            for category in self.youtube_categories
        ]
        languages = [
            {"term": {"language": language}}
            for language in self.languages
        ]

        # Add filters to refs
        category_score_filters.extend(category_score_params)
        language_filters.extend(languages)
        youtube_categories_filters.extend(youtube_categories)

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
            threshold = 89
        elif score_threshold == 2:
            threshold = 75
        elif score_threshold == 3:
            threshold = 50
        else:
            threshold = 100
        return threshold

    @staticmethod
    def map_youtube_categories(youtube_category_ids):
        mapping = {
            _id: category.lower() for _id, category in AuditCategory.get_all().items()
        }
        to_string = [mapping[str(_id)] for _id in youtube_category_ids]
        return to_string

    def _get_related_key(self):
        """
        Interface to get key to retrieve related items
        :return: str
        """
        config = {
            constants.VIDEO: "channel_id"
        }
        key = config[self.segment_type]
        return key
