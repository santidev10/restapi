from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView


from es_components.connections import init_es_connection
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager

from channel.api.mixins import ChannelYoutubeStatisticsMixin
from channel.api.views import ChannelListApiView
from channel.models import AuthChannel
from userprofile.models import UserChannel
from utils.celery.dmp_celery import send_task_delete_channels
from utils.permissions import OnlyAdminUserOrSubscriber
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.brand_safety_view_decorator import add_brand_safety_data

init_es_connection()

PERMITTED_CHANNEL_GROUPS = ("influencers", "new", "media", "brands",)


class OwnChannelPermissions(BasePermission):
    def has_permission(self, request, view):
        return UserChannel.objects \
            .filter(user=request.user, channel_id=view.kwargs.get("pk")) \
            .exists()


class ChannelRetrieveUpdateDeleteApiView(APIView, PermissionRequiredMixin, ChannelYoutubeStatisticsMixin):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.channel_details"),
            OwnChannelPermissions,
            OnlyAdminUserOrSubscriber),)

    __channel_manager = None
    video_manager = VideoManager((Sections.GENERAL_DATA, Sections.STATS))

    def channel_manager(self, sections=None):
        if sections or self.channel_manager is None:
            self.__channel_manager = ChannelManager(sections)
        return self.__channel_manager

    def put(self, *args, **kwargs):
        data = self.request.data

        if "channel_group" in data and data["channel_group"] not in PERMITTED_CHANNEL_GROUPS:
            return Response(status=HTTP_400_BAD_REQUEST)

        channel_id = kwargs.get("pk")
        channel = self.channel_manager(Sections.CUSTOM_PROPERTIES).get([channel_id])

        if not channel:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        channel = channel[0]
        channel.populate_custom_properties(**data)
        self.channel_manager().upsert([channel])

        return self.get(*args, **kwargs)

    @add_brand_safety_data
    def get(self, *args, **kwargs):
        if self.request.user.is_staff and self.request.query_params.get("from_youtube") == "1":
            return self.obtain_youtube_statistics()

        channel_id = kwargs.get('pk')
        allowed_sections_to_load = (Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES,
                                    Sections.STATS, Sections.ADS_STATS)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))
        if channel_id in user_channels or self.request.user.has_perm("userprofile.channel_audience")\
                or self.request.user.is_staff:
            allowed_sections_to_load += (Sections.ANALYTICS,)

        channel = self.channel_manager(allowed_sections_to_load).get([channel_id])

        if not channel:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        channel = channel[0]

        video_filters = self.video_manager.by_channel_ids_query(channel_id) & self.video_manager.forced_filters()
        videos = self.video_manager.search(
            filters=video_filters,
            sort=self.get_video_sort_rule(),
            limit=50
        ).execute().hits

        average_views = 0

        if len(videos):
            average_views = round(
                sum([video.stats.views or 0 for video in videos]) / len(videos)
            )

        result = ChannelListApiView.add_chart_data(channel.to_dict(skip_empty=False))
        result.update({
            "performance": {
                "average_views": average_views,
                "videos": [video.to_dict(skip_empty=False) for video in videos],
            }
        })

        return Response(result)

    def delete(self, *args, **kwargs):
        channel_id = kwargs.get("pk")

        UserChannel.objects \
            .filter(channel_id=channel_id, user=self.request.user) \
            .delete()

        if not UserChannel.objects.filter(channel_id=channel_id).exists():
            AuthChannel.objects.filter(channel_id=channel_id).delete()
            send_task_delete_channels(([channel_id],))

        return Response()

    def get_video_sort_rule(self):
        return [{"general_data.youtube_published_at": {"order": SortDirections.DESCENDING}}]
