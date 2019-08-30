import re
from distutils.util import strtobool

from django.core.paginator import EmptyPage
from django.core.paginator import InvalidPage
from django.core.paginator import Paginator
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.views import APIView

import brand_safety.constants as constants
from brand_safety.auditors.utils import AuditUtils
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from utils.brand_safety_view_decorator import get_brand_safety_data
from utils.brand_safety_view_decorator import get_brand_safety_label

REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"


class BrandSafetyChannelAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 89
    MAX_PAGE_SIZE = 24
    channel_manager = ChannelManager(sections=(Sections.STATS, Sections.BRAND_SAFETY))
    video_manager = VideoManager(sections=(Sections.BRAND_SAFETY,))

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

        channel_data = AuditUtils.get_items([channel_id], self.channel_manager)[0]
        channel_response = {
            "total_videos_scored": 0,
            "total_flagged_videos": 0,
            "flagged_words": 0,
        }

        try:
            # Add brand safety data to channel response
            channel_response.update({
                "total_videos_scored": channel_data.brand_safety.videos_scored,
                "flagged_words": self._extract_key_words(channel_data.brand_safety.categories.to_dict()),
                **get_brand_safety_data(channel_data.brand_safety.overall_score)
            })
        except (IndexError, AttributeError):
            # No channel brand safety, add empty data
            channel_response.update({
                "total_videos_scored": channel_data.stats.total_videos_count or 0,
                "flagged_words": 0,
                **get_brand_safety_data(None)
            })
        try:
            videos = self._get_channel_video_data(channel_id)
        except Exception as e:
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)

        # Add flagged videos to channel brand safety
        flagged_videos = []
        for video in videos:
            try:
                video_overall_score = video.brand_safety.overall_score
                if video_overall_score is None:
                    continue
                if video_overall_score <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                    flagged_videos.append(self._extract_video_data(video))
            except AttributeError:
                continue
        channel_response["total_flagged_videos"] = len(flagged_videos)
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
        response = self._adapt_response_data(channel_response, paginator, page)
        return Response(status=HTTP_200_OK, data=response)

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
                          "captions", "brand_safety.overall_score")

        by_channel_filter = self.video_manager.by_channel_ids_query(channel_id)
        videos = self.video_manager.search(filters=by_channel_filter,
                                           limit=self.MAX_SIZE,
                                           sort=[{"main.id": {"order": SortDirections.ASCENDING}}]). \
            source(includes=fields_to_load).execute().hits
        return videos

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

    def _extract_video_data(self, video):
        """
        Extract video brand safety data
        :param video:
        :return:
        """
        brand_safety_score = video.brand_safety.overall_score
        brand_safety_label = get_brand_safety_label(brand_safety_score)
        data = {
            "id": video.main.id,
            "score": video.brand_safety.overall_score,
            "label": brand_safety_label,
            "title": video.general_data.title,
            "thumbnail_image_url": video.general_data.thumbnail_image_url,
            "transcript": self.__get_transcript(video.captions),
            "youtube_published_at": video.general_data.youtube_published_at,
            "views": video.stats.views,
            "engage_rate": video.stats.engage_rate
        }
        return data
