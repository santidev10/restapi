from django.http import Http404
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.response import Response

from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.brand_safety_view_decorator import get_brand_safety_label
from brand_safety.models import BadWordCategory
import brand_safety.constants as constants


class BrandSafetyVideoAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000

    def get(self, request, **kwargs):
        """
        View to retrieve individual video brand safety data
        """
        video_id = kwargs["pk"]
        category_mapping = BadWordCategory.get_category_mapping()
        try:
            video_es_data = ElasticSearchConnector(index_name=constants.BRAND_SAFETY_VIDEO_ES_INDEX).search_by_id(
                constants.BRAND_SAFETY_VIDEO_ES_INDEX,
                video_id,
                constants.BRAND_SAFETY_SCORE_TYPE)
        except ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data="Brand Safety data unavailable.")
        if not video_es_data:
            raise Http404
        video_score = video_es_data["overall_score"]
        video_brand_safety_data = {
            "score": video_score,
            "label": get_brand_safety_label(video_score),
            "total_unique_flagged_words": 0,
            "unique_flagged_words": [],
            "category_flagged_words": {},
            "transcript_flagged_words": {
                category: [] for category in category_mapping.keys()
            },
        }
        try:
            # Put transcript words in appropriate response category
            for keyword in video_es_data["transcript_hits"]:
                category = keyword["category"]
                video_brand_safety_data[category].append(keyword["word"])
        except KeyError:
            pass
        # Map category ids to category strings
        video_brand_safety_data["transcript_flagged_words"] = {
            category_mapping[category_id]: words
            for category_id, words in video_brand_safety_data["transcript_flagged_words"].items()
        }
        # Map category ids to category names and aggregate all keywords for each category
        for category_id, data in video_es_data["categories"].items():
            category_name = category_mapping[category_id]
            keywords = [word["keyword"] for word in data["keywords"]]
            video_brand_safety_data["unique_flagged_words"].extend(keywords)
            video_brand_safety_data["total_unique_flagged_words"] += len(keywords)
            video_brand_safety_data["category_flagged_words"][category_name] = len(keywords)
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)
