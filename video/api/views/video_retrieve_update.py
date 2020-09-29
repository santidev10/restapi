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
from es_components.managers import ChannelManager
from es_components.managers.video import VideoManager
from utils.es_components_api_utils import get_fields
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.utils import prune_iab_categories
from video.api.serializers.video import VideoAdminSerializer


class VideoRetrieveUpdateApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    permission_required = ("userprofile.video_details",)

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

        fields_to_load = get_fields(request.query_params, allowed_sections_to_load)
        try:
            video = self.video_manager(allowed_sections_to_load).model.get(video_id, _source=fields_to_load)
        except NotFoundError:
            return Response(data={"error": "Video not found"}, status=HTTP_404_NOT_FOUND)

        if not video:
            return Response(data={"error": "Video not found"}, status=HTTP_404_NOT_FOUND)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))

        context = self._get_serializer_context(video.channel.id)
        result = VideoAdminSerializer(video, context=context).data
        try:
            result["general_data"]["iab_categories"] = prune_iab_categories(result["general_data"]["iab_categories"])
        # pylint: disable=broad-except
        except Exception:
            pass
        # pylint: enable=broad-except

        if not (video.channel.id in user_channels or self.request.user.has_perm("userprofile.video_audience")
                or self.request.user.is_staff):
            if Sections.ANALYTICS in result.keys():
                del result[Sections.ANALYTICS]

        return Response(result)

    def _get_serializer_context(self, channel_id):
        try:
            channel = ChannelManager(Sections.CUSTOM_PROPERTIES).get([channel_id], skip_none=True)[0]
            channel_blocklist = channel.custom_properties.blocklist
        except (IndexError, RequestError):
            channel_blocklist = False
        context = {
            "channel_blocklist": {
                channel_id: channel_blocklist
            }
        }
        return context
