"""
Video api views module
"""

from django.contrib.auth.mixins import PermissionRequiredMixin
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from es_components.constants import Sections
from es_components.languages import LANGUAGES
from es_components.managers import ChannelManager
from es_components.managers.video import VideoManager
from userprofile.constants import StaticPermissions
from utils.api.mutate_query_params import AddFieldsMixin
from utils.es_components_api_utils import get_fields
from utils.utils import prune_iab_categories
from video.api.serializers.video import VideoSerializer
from video.api.views.video_view_transcript_mixin import VideoTranscriptSerializerContextMixin


class VideoRetrieveUpdateApiView(APIView, PermissionRequiredMixin, AddFieldsMixin,
                                 VideoTranscriptSerializerContextMixin):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA),)

    __video_manager = VideoManager

    def video_manager(self, sections=None):
        if sections or self.__video_manager is None:
            self.__video_manager = VideoManager(sections)
        return self.__video_manager

    def get(self, request, *args, **kwargs):
        video_id = kwargs.get("pk")

        allowed_sections_to_load = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION,
                                    Sections.CAPTIONS, Sections.ANALYTICS, Sections.BRAND_SAFETY,
                                    Sections.CUSTOM_CAPTIONS, Sections.CUSTOM_PROPERTIES, Sections.TASK_US_DATA)

        self.add_fields()

        fields_to_load = get_fields(request.query_params, allowed_sections_to_load)
        try:
            video = self.video_manager(allowed_sections_to_load).model.get(video_id, _source=fields_to_load)
        except NotFoundError:
            return Response(data={"error": "Video not found"}, status=HTTP_404_NOT_FOUND)

        if not video:
            return Response(data={"error": "Video not found"}, status=HTTP_404_NOT_FOUND)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))

        context = self._get_serializer_context(channel_id=video.channel.id, video_id=video.main.id)
        result = VideoSerializer(video, context=context).data

        try:
            result["general_data"]["iab_categories"] = prune_iab_categories(result["general_data"]["iab_categories"])
        # pylint: disable=broad-except
        except Exception:
            pass
        # pylint: enable=broad-except

        if not (video.channel.id in user_channels
                or self.request.user.has_permission(StaticPermissions.RESEARCH__AUTH)):
            if Sections.ANALYTICS in result.keys():
                del result[Sections.ANALYTICS]

        return Response(result)

    def _get_serializer_context(self, channel_id: str, video_id: str) -> dict:
        try:
            channel = ChannelManager(Sections.CUSTOM_PROPERTIES).get([channel_id], skip_none=True)[0]
            channel_blocklist = channel.custom_properties.blocklist
        except (IndexError, RequestError):
            channel_blocklist = False
        context = {
            "user": self.request.user,
            "channel_blocklist": {
                channel_id: channel_blocklist
            },
            "languages_map": {code.lower(): name for code, name in LANGUAGES.items()},
            "transcripts": self.get_transcripts_serializer_context(video_ids=[video_id]),
        }
        return context
