import re
from django.conf import settings
from django.core.paginator import Paginator
from django.core.paginator import InvalidPage
from django.core.paginator import EmptyPage
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.response import Response

from es_components.managers.video import VideoManager
from es_components.constants import SortDirections
from distutils.util import strtobool
from brand_safety.utils import get_es_data
from brand_safety.utils import BrandSafetyQueryBuilder
from brand_safety.models import BadWordCategory
import brand_safety.constants as constants
from utils.elasticsearch import ElasticSearchConnectorException
from utils.brand_safety_view_decorator import get_brand_safety_data

REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"


class BrandSafetyChannelAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 89
    MAX_PAGE_SIZE = 24
    video_manager = VideoManager()

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
        try:
            videos = self._get_channel_video_data(channel_id)
        except Exception as e:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not channel_es_data:
            raise Http404

        # Retrieve channel flagged videos
        brand_safety_params = {
            "list_type": "blacklist",
            "segment_type": "video",
        }
        query_builder = BrandSafetyQueryBuilder(
            brand_safety_params,
            overall_score=self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD,
            video_ids=list(videos.keys())
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

        if video_es_data is ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)

        channel_brand_safety_data = {
            "total_videos_scored": channel_es_data["videos_scored"],
            "total_flagged_videos": 0,
            "flagged_words": self._extract_key_words(channel_es_data["categories"])
        }
        channel_brand_safety_data.update(get_brand_safety_data(channel_es_data["overall_score"]))
        # Merge es brand safety with sdb video data
        channel_brand_safety_data, flagged_videos = self._adapt_channel_video_es_sdb_data(channel_brand_safety_data, video_es_data, videos)
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
        paginator = Paginator(flagged_videos, size)
        response = self._adapt_response_data(channel_brand_safety_data, paginator, page)
        return Response(status=HTTP_200_OK, data=response)

    def _adapt_channel_video_es_sdb_data(self, channel_data:dict, video_es_data: dict, videos: dict):
        """
        Encapsulate merging of channel and video es/sdb data
        :param es_data: dict
        :param sdb_data: dict
        :return: tuple -> channel brand safety data, flagged videos
        """
        flagged_videos = []
        # Merge video brand safety wtih video sdb data
        for video_id, data in video_es_data.items():
            if data.get("overall_score") and data.get("overall_score") <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                video = videos.get(video_id)
                video_brand_safety_data = get_brand_safety_data(data["overall_score"])
                video_data = {
                    "id": video_id,
                    "score": video_brand_safety_data["score"],
                    "label": video_brand_safety_data["label"],
                    "title": video.general_data.title,
                    "thumbnail_image_url": video.general_data.thumbnail_image_url,
                    "transcript": self.__get_transcript(video.captions),
                    "youtube_published_at": video.general_data.youtube_published_at,
                    "views": video.stats.views,
                    "engage_rate": video.stats.engage_rate
                }
                flagged_videos.append(video_data)
                channel_data["total_flagged_videos"] += 1
        flagged_videos.sort(key=lambda video: video["youtube_published_at"], reverse=True)
        return channel_data, flagged_videos

    def __get_transcript(self, captions):
        if captions and captions.items:
            for caption in captions.items:
                text = caption.text
                if caption.language_code == "en" and text:
                    transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
                    return transcript

    def _get_channel_video_data(self, channel_id):
        fields_to_load = ("main.id", "general_data.title", "general_data.thumbnail_image_url",
                          "general_data.youtube_published_at", "stats.views", "stats.engage_rate",
                          "captions")

        by_channel_filter = self.video_manager.by_channel_ids_query(channel_id)
        videos = self.video_manager.search(filters=by_channel_filter,
                                           limit=self.MAX_SIZE,
                                           sort=[{"main.id": {"order": SortDirections.ASCENDING}}]).\
            source(includes=fields_to_load).execute().hits

        video_data = {
            video.main.id: video
            for video in videos
        }
        return video_data

    @staticmethod
    def _adapt_response_data(brand_safety_data: dict, paginator: Paginator, page: int) -> dict:
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

    def _extract_key_words(self, categories: dict) -> list:
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
