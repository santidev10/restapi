from django.conf import settings

from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView


class SegmentWizardAPIView(APIView):
    REQUIRED_CUSTOM_SEGMENT_FIELDS = "type, content, score_threshold, brand_safety_categories, youtube_categories"

    def post(self, request, *args, **kwargs):
        query = {
            "query": {
                "bool": {
                    "must": {
                        "nested": {
                            "path": "categories",
                            "query": {
                                "bool": {
                                    "filter": []
                                }
                            }
                        }
                    },
                    "should": []
                }
            }
        }
        data = request.data
        segment_filters = query["query"]["bool"]["should"]
        category_score_filters = query["query"]["bool"]["must"]["nested"]["query"]["bool"]["filter"]
        try:
            segment_type = data["type"]
            segment_content = data["segment_content"]
            languages = data["langauges"]
            youtube_categories = data["youtube_categories"]
            score_threshold = data["score_threshold"]
        except KeyError:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data="You must provide these fields: {}".format(self.REQUIRED_CUSTOM_SEGMENT_FIELDS))

        config = self._get_segment_config(segment_content, segment_type)
        if config is None:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid options: {}".format(",".join([segment_type, segment_content])))

        # {"range": {"categories.1.category_score": {"gte": 50}}}
        category_score_params = [
            {
                "range": {"categories.{}.category_score".format(category): {range_operator: score_threshold}}
            } for category in data["brand_safety_categories"]
        ]
        youtube_categories = [
            {
                "term": {"youtube_category": category}
            } for category in youtube_categories
        ]
        languages = [
            {
                "term": {"language": language}
            } for language in languages
        ]

        category_score_filters.extend(category_score_params)
        segment_filters.extend(youtube_categories)
        segment_filters.extend(languages)




    def _get_segment_config(self, segment_content, segment_type):
        options = {
            "channel": {
                "index": settings.BRAND_SAFETY_CHANNEL_INDEX,
                "minimum_option": "subscribers"
            },
            "video": {
                "index": settings.BRAND_SAFETY_VIDEO_INDEX,
                "minimum_option": "views"
            },
            "range_operator": {
                "blacklist": "lte",
                "whitelist": "gte"
            },
        }
        try:
            config = {
                "index": options[segment_content]["index"],
                "minimum_option": options[segment_content]["minimum_option"],
                "range_operator": options[segment_type],
            }
        except KeyError:
            config = None
        return config


# {
#     "type": "whitelist",
#     "content": "channels",
#     "score_threshold": 50,
#     "brand_safety_categories": [],
#     "youtube_categories": [],
#     "languages": [],
#     "minimum_filter": 40 # will either be subscribers for channels or views for videos
# }
#
# {
# 	"query": {
# 		"bool": {
# 			"filter": [
# 				{ "range": { "categories.1.category_score": { "gte": 50 } } }
# 			],
# 			"should": [
# 				{ "term": { "youtube_category": "sports" } },
# 				{ "term": { "youtube_category": "education" } },
# 				{ "term": { "language": "es" } }
# 			]
# 		}
# 	}
# }
#
# WORKS
# {
# 	"query": {
# 		"nested": {
# 			"path": "categories",
# 			"query": {
# 				"bool": {
# 					"filter": [
# 						{ "range": { "categories.1.category_score": { "gte": 100 } } }
# 					]
# 				}
# 			}
# 		}
# 	}
# }

# WORKS
# {
# 	"query": {
# 		"bool": {
# 			"must": {
# 				"nested": {
# 					"path": "categories",
# 					"query": {
# 						"bool": {
# 							"filter": [
# 								{ "range": { "categories.1.category_score": { "gte": 100 } } },
# 								{ "range": { "categories.2.category_score": { "gte": 100 } } },
# 								{ "range": { "categories.3.category_score": { "gte": 100 } } },
# 								{ "range": { "categories.4.category_score": { "gte": 100 } } }
# 							]
# 						}
# 					}
# 				}
# 			},
# 			"should": [
# 				{ "term": { "youtube_category": "sports" } },
# 				{ "term": { "youtube_category": "education" } },
# 				{ "term": { "language": "es" } }
# 			]
# 		}
# 	}
# }