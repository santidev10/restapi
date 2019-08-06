"""
Video api views module
"""
import re
from datetime import timedelta
from datetime import datetime

from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView
from singledb.settings import DEFAULT_VIDEO_DETAILS_FIELDS
from es_components.managers.video import VideoManager
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.es_components_api_utils import get_fields

from es_components.constants import Sections


REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"


def add_transcript(video):
    transcript = None
    if video.get("captions") and video["captions"].get("items"):
        for caption in video["captions"].get("items"):
            if caption.get("language_code") == "en":
                text = caption.get("text")
                transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
    video["transcript"] = transcript
    return video


def add_chart_data(video):
    if not video.get("stats"):
        video["chart_data"] = []
        return video

    chart_data = []
    items_count = 0
    history = zip(
        reversed(video["stats"].get("views_history") or []),
        reversed(video["stats"].get("likes_history") or []),
        reversed(video["stats"].get("dislikes_history") or []),
        reversed(video["stats"].get("comments_history") or [])
    )
    for views, likes, dislikes, comments in history:
        timestamp = video["stats"].get("historydate") - timedelta(
                days=len(video["stats"].get("views_history")) - items_count - 1)
        timestamp = datetime.combine(timestamp, datetime.max.time())
        items_count += 1
        if any((views, likes, dislikes, comments)):
            chart_data.append(
                {"created_at": "{}{}".format(str(timestamp), "Z"),
                 "views": views,
                 "likes": likes,
                 "dislikes": dislikes,
                 "comments": comments}
            )
    video["chart_data"] = chart_data
    return video


def add_extra_field(video):
    video = add_chart_data(video)
    video = add_transcript(video)
    return video


class VideoRetrieveUpdateApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    permission_required = ("userprofile.video_details",)
    default_request_fields = DEFAULT_VIDEO_DETAILS_FIELDS

    __video_manager = VideoManager

    def video_manager(self, sections=None):
        if sections or self.__video_manager is None:
            self.__video_manager = VideoManager(sections)
        return self.__video_manager

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        video_id = kwargs.get('pk')

        allowed_sections_to_load = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION,
                                    Sections.CAPTIONS, Sections.ANALYTICS)

        fields_to_load = get_fields(request.query_params, allowed_sections_to_load)

        video = self.video_manager(allowed_sections_to_load).model.get(video_id, _source=fields_to_load)

        if not video:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))

        result = add_extra_field(video.to_dict())

        if not(video.channel.id in user_channels or self.request.user.has_perm("userprofile.video_audience") \
                or self.request.user.is_staff):
            result[Sections.ANALYTICS] = {}

        return Response(result)