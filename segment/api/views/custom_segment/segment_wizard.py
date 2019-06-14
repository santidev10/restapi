from rest_framework.views import APIView

class SegmentWizardAPIView(APIView):
    pass


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