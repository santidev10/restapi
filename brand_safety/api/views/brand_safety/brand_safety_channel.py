from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.response import Response
from django.core.paginator import Paginator
from django.core.paginator import InvalidPage
from django.core.paginator import EmptyPage
from django.http import Http404

from utils.elasticsearch import ElasticSearchConnectorException
from utils.brand_safety_view_decorator import get_brand_safety_data
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from brand_safety.api.views.brand_safety.utils.utils import get_es_data
import brand_safety.constants as constants


class BrandSafetyChannelAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 89
    MAX_PAGE_SIZE = 50

    def get(self, request, **kwargs):
        """
        View to retrieve individual channel brand safety data
        """
        channel_id = kwargs["pk"]
        query_params = request.query_params
        page = query_params.get("page", 1)
        try:
            threshold = int(query_params["threshold"])
            self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD = threshold
        except (ValueError, KeyError):
            pass
        try:
            size = int(query_params["size"])
            if size <= 0 or size > self.MAX_PAGE_SIZE:
                size = self.MAX_PAGE_SIZE
        except (ValueError, TypeError, KeyError):
            size = self.MAX_PAGE_SIZE

        channel_es_data = get_es_data(channel_id, constants.BRAND_SAFETY_CHANNEL_ES_INDEX)
        if isinstance(channel_es_data, ElasticSearchConnectorException):
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not channel_es_data:
            raise Http404

        # Get channel's video ids from sdb to get es video brand safety data
        video_sdb_data = self._get_sdb_channel_video_data(channel_id)
        if isinstance(video_sdb_data, SingleDatabaseApiConnectorException):
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)

        # Get video brand safety data to merge with sdb video data
        video_ids = list(video_sdb_data.keys())
        video_es_data = get_es_data(video_ids, constants.BRAND_SAFETY_VIDEO_ES_INDEX)
        if isinstance(video_es_data, ElasticSearchConnectorException):
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)

        channel_brand_safety_data = {
            "total_videos_scored": channel_es_data["videos_scored"],
            "total_flagged_videos": 0,
            "flagged_words": self._extract_key_words(channel_es_data["categories"])
        }
        channel_brand_safety_data.update(get_brand_safety_data(channel_es_data["overall_score"]))

        channel_brand_safety_data, flagged_videos = self._adapt_channel_video_es_sdb_data(channel_brand_safety_data, video_es_data, video_sdb_data)
        paginator = Paginator(flagged_videos, size)
        response = self._adapt_response_data(channel_brand_safety_data, paginator, page)
        return Response(status=HTTP_200_OK, data=response)

    def _adapt_channel_video_es_sdb_data(self, channel_data, video_es_data, video_sdb_data):
        """
        Encapsulate merging of channel and video es/sdb data
        :param es_data: dict
        :param sdb_data: dict
        :return: tuple -> channel brand safety data, flagged videos
        """
        flagged_videos = []
        # Merge video brand safety wtih video sdb data
        for id_, data in video_es_data.items():
            if data["overall_score"] <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                # In some instances video data will not be in both Elasticsearch and sdb
                try:
                    sdb_video = video_sdb_data[id_]
                except KeyError:
                    sdb_video = {}
                video_brand_safety_data = get_brand_safety_data(data["overall_score"])
                video_data = {
                    "id": id_,
                    "score": video_brand_safety_data["score"],
                    "label": video_brand_safety_data["label"],
                    "title": sdb_video.get("title"),
                    "thumbnail_image_url": sdb_video.get("thumbnail_image_url"),
                    "transcript": sdb_video.get("transcript"),
                }
                flagged_videos.append(video_data)
                channel_data["total_flagged_videos"] += 1
        return channel_data, flagged_videos

    def _get_sdb_channel_video_data(self, channel_id):
        """
        Encapsulate getting sdb channel video data
            On SingleDatabaseApiConnectorException, return it to be handled by view
        :param channel_id: str
        :return: dict or SingleDatabaseApiConnectorException
        """
        params = {
            "fields": "video_id,title,transcript,thumbnail_image_url",
            "sort": "video_id",
            "size": self.MAX_SIZE,
            "channel_id__terms": channel_id
        }
        try:
            response = SingleDatabaseApiConnector().get_video_list(params)
        except SingleDatabaseApiConnectorException:
            return SingleDatabaseApiConnectorException
        sdb_video_data = {
            video["video_id"]: video
            for video in response["items"]
        }
        return sdb_video_data

    @staticmethod
    def _adapt_response_data(brand_safety_data, paginator, page):
        """
        Adapt brand safety data with pagination
        :param brand_safety_data: channel brand safety data
        :param paginator: django Paginator instance
        :param page: int
        :return: dict
        """
        try:
            page_items = paginator.page(page)
        except EmptyPage:
            page_items = paginator.page(paginator.num_pages)
        except InvalidPage:
            page_items = paginator.page(1)
        response = {
            "brand_safety": brand_safety_data,
            "current_page": page_items.number,
            "items": page_items.object_list,
            "items_count": paginator.count,
            "max_page": paginator.num_pages,
        }
        return response

    @staticmethod
    def _extract_key_words(categories):
        """
        Extracts es brand safety category keywords
        :param categories: dict
        :return: list
        """
        keywords = []
        for keyword_data in categories.values():
            keywords.extend([item["keyword"] for item in keyword_data["keywords"]])
        return keywords


