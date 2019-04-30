from django.http import Http404
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_500_INTERNAL_SERVER_ERROR
from rest_framework.response import Response

from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
import brand_safety.constants as constants
from brand_safety.models import BadWordCategory


class BrandSafetyVideoAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    category_mapping = BadWordCategory.get_category_mapping()
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 70

    def get(self, request, **kwargs):
        video_id = kwargs["pk"]
        video_brand_safety_data = {}
        try:
            video_es_data = ElasticSearchConnector(index_name=constants.BRAND_SAFETY_VIDEO_ES_INDEX).search_by_id(
                constants.BRAND_SAFETY_VIDEO_ES_INDEX,
                video_id,
                constants.BRAND_SAFETY_SCORE_TYPE)
        except ElasticSearchConnectorException:
            return Response(status=HTTP_500_INTERNAL_SERVER_ERROR, data="Brand Safety data unavailable.")
        if not video_es_data:
            raise Http404
        flagged_words = {
            "all_words": []
        }
        for category_id, data in video_es_data["categories"].items():
            category_name = self.category_mapping[category_id]
            keywords = data["keywords"]
            flagged_words[category_name] = len(keywords)
            flagged_words["all_words"].extend(keywords)
        video_brand_safety_data["flagged_words"] = flagged_words
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)




