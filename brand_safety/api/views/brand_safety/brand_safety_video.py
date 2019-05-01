from django.http import Http404
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.response import Response

from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
import brand_safety.constants as constants
from brand_safety.models import BadWordCategory
from utils.brand_safety_view_decorator import get_brand_safety_label


class BrandSafetyVideoAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    category_mapping = BadWordCategory.get_category_mapping()
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 70

    def get(self, request, **kwargs):
        """
        View to retrieve individual video brand safety data
        """
        video_id = kwargs["pk"]
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
            "label": get_brand_safety_label(video_score)
        }
        flagged_words = {
            "all_words": []
        }
        # Map category ids to category names and aggregate all keywords for each category
        for category_id, data in video_es_data["categories"].items():
            category_name = self.category_mapping[category_id]
            keywords = [word["keyword"] for word in data["keywords"]]
            flagged_words[category_name] = len(keywords)
            flagged_words["all_words"].extend(keywords)
        video_brand_safety_data["flagged_words"] = flagged_words
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)




