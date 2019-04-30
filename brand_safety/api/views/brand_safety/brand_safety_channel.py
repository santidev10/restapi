from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from django.http import Http404
from rest_framework.response import Response

from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
import brand_safety.constants as constants
from utils.brand_safety_view_decorator import get_brand_safety_label


class BrandSafetyChannelAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    es_connector = ElasticSearchConnector()
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 70

    def get(self, request, **kwargs):
        """
        View to retrieve individual channel brand safety data
        """
        channel_id = kwargs["pk"]
        try:
            channel_es_data = self.es_connector.search_by_id(
                constants.BRAND_SAFETY_CHANNEL_ES_INDEX,
                channel_id,
                constants.BRAND_SAFETY_SCORE_TYPE)
        except ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not channel_es_data:
            raise Http404
        channel_score = channel_es_data["overall_score"]
        channel_brand_safety_data = {
            "brand_safety": {
                "score": channel_score,
                "label": get_brand_safety_label(channel_score)
            },
            "videos_scored": channel_es_data["videos_scored"],
            "flagged_videos": [],
            "flagged_videos_count": 0
        }
        # Get channel's video ids from sdb to get es video brand safety data
        try:
            params = {
                "fields": "video_id,title,transcript",
                "sort": "video_id",
                "size": self.MAX_SIZE,
                "channel_id__terms": channel_id
            }
            response = SingleDatabaseApiConnector().get_video_list(params)
            sdb_video_data = {
                video["video_id"]: video
                for video in response["items"]
            }
        except SingleDatabaseApiConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        video_ids = list(sdb_video_data.keys())
        video_es_data = self.es_connector.search_by_id(
            constants.BRAND_SAFETY_VIDEO_ES_INDEX,
            video_ids,
            constants.BRAND_SAFETY_SCORE_TYPE)
        for id_, data in video_es_data.items():
            # if flagged, then merge data and append flagged videos
            if data["overall_score"] <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                channel_brand_safety_data["flagged_videos"].append(data)
                channel_brand_safety_data["flagged_videos_count"] += 1
        return Response(status=HTTP_200_OK, data=channel_brand_safety_data)
