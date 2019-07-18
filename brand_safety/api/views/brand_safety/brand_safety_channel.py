from django.conf import settings
from django.core.paginator import Paginator
from django.core.paginator import InvalidPage
from django.core.paginator import EmptyPage
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.response import Response

from distutils.util import strtobool
from brand_safety.utils import get_es_data
from brand_safety.utils import BrandSafetyQueryBuilder
from brand_safety.models import BadWordCategory
import brand_safety.constants as constants
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.elasticsearch import ElasticSearchConnectorException
from utils.brand_safety_view_decorator import get_brand_safety_data
from audit_tool.models import BlacklistItem

class BrandSafetyChannelAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 89
    MAX_PAGE_SIZE = 24

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

        channel_es_data = get_es_data(channel_id, settings.BRAND_SAFETY_CHANNEL_INDEX)
        if channel_es_data is ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not channel_es_data:
            raise Http404

        # Retrieve channel flagged videos
        query_params = {
            "list_type": "whitelist",
            "segment_type": "video",
        }
        query_builder = BrandSafetyQueryBuilder(
            query_params,
            overall_score=self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD,
            related_to=channel_id
        )
        result = query_builder.execute()
        if result is ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not channel_es_data:
            raise Http404

        # Extract data from es response
        video_es_data = {
            video["_id"]: video["_source"] for video in result["hits"]["hits"]
        }
        video_sdb_data = self._get_sdb_video_data(video_es_data.keys())
        if video_sdb_data is SingleDatabaseApiConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)

        channel_brand_safety_data = {
            "total_videos_scored": channel_es_data["videos_scored"],
            "total_flagged_videos": 0,
            "flagged_words": self._extract_key_words(channel_es_data["categories"])
        }
        channel_brand_safety_data.update(get_brand_safety_data(channel_es_data["overall_score"]))
        # Merge es brand safety with sdb video data
        channel_brand_safety_data, flagged_videos = self._adapt_channel_video_es_sdb_data(channel_brand_safety_data, video_es_data, video_sdb_data)
        # Sort video responses if parameter is passed in
        sort_options = ["youtube_published_at", "score", "views", "engage_rate"]
        sorting = query_params['sort'] if "sort" in query_params else None
        ascending = query_params['sortAscending'] if "sortAscending" in query_params else None
        if ascending is not None:
            try:
                ascending = strtobool(ascending)
            except Exception as e:
                raise ValueError("Expected sortAscending to be boolean value. Received {}".format(ascending))
        reverse = True
        if ascending:
            reverse = not ascending
        if sorting in sort_options:
            flagged_videos.sort(key=lambda video: video[sorting], reverse=reverse)
        #channel_brand_safety_data["blacklist_data"] = BlacklistItem.get(channel_id, BlacklistItem.CHANNEL_ITEM, to_dict=True)
        paginator = Paginator(flagged_videos, size)
        response = self._adapt_response_data(channel_brand_safety_data, paginator, page)
        return Response(status=HTTP_200_OK, data=response)

    def _adapt_channel_video_es_sdb_data(self, channel_data: dict, video_es_data: dict, video_sdb_data: dict):
        """
        Encapsulate merging of channel and video es/sdb data
        :param es_data: dict
        :param sdb_data: dict
        :return: tuple -> channel brand safety data, flagged videos
        """
        flagged_videos = []
        # Merge video brand safety wtih video sdb data
        for _id, data in video_es_data.items():
            if data["overall_score"] <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                # In some instances video data will not be in both Elasticsearch and sdb
                try:
                    sdb_video = video_sdb_data[_id]
                except KeyError:
                    sdb_video = {}
                video_brand_safety_data = get_brand_safety_data(data["overall_score"])
                video_data = {
                    "id": _id,
                    "score": video_brand_safety_data["score"],
                    "label": video_brand_safety_data["label"],
                    "title": sdb_video.get("title"),
                    "thumbnail_image_url": sdb_video.get("thumbnail_image_url"),
                    "transcript": sdb_video.get("transcript"),
                    "youtube_published_at": sdb_video.get("youtube_published_at", ""),
                    "views": sdb_video.get("views"),
                    "engage_rate": sdb_video.get("engage_rate", 0)
                }
                flagged_videos.append(video_data)
                channel_data["total_flagged_videos"] += 1
        flagged_videos.sort(key=lambda video: video["youtube_published_at"], reverse=True)
        return channel_data, flagged_videos

    def _get_sdb_channel_video_data(self, channel_id: str):
        """
        Encapsulate getting sdb channel video data
            On SingleDatabaseApiConnectorException, return it to be handled by view
        :param channel_id: str
        :return: dict or SingleDatabaseApiConnectorException
        """
        params = {
            "fields": "video_id,title,transcript,thumbnail_image_url,youtube_published_at,views,engage_rate",
            "sort": "video_id",
            "size": self.MAX_SIZE,
            "channel__id_terms": channel_id
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

    def _get_sdb_video_data(self, video_ids: iter):
        params = {
            "fields": "video_id,title,transcript,thumbnail_image_url,youtube_published_at,views,engage_rate",
            "sort": "video_id",
            "size": self.MAX_SIZE,
            "video_id__terms": ",".join(video_ids)
        }
        try:
            response = SingleDatabaseApiConnector().get_video_list(params, ignore_sources=True)
        except SingleDatabaseApiConnectorException:
            return SingleDatabaseApiConnectorException
        sdb_video_data = {
            video["video_id"]: video
            for video in response["items"]
        }
        return sdb_video_data

    @staticmethod
    def _adapt_response_data(brand_safety_data: dict, paginator: Paginator, page: int):
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

    def _extract_key_words(self, categories: dict):
        """
        Extracts es brand safety category keywords
        :param categories: dict
        :return: list
        """
        keywords = []
        for category_id, keyword_data in categories.items():
            if category_id in BadWordCategory.EXCLUDED:
                continue
            keywords.extend([item["keyword"] for item in keyword_data["keywords"]])
        return keywords
