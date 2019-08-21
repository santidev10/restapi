from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView
from elasticsearch.exceptions import NotFoundError

from channel.api.mixins import ChannelYoutubeStatisticsMixin
from channel.api.serializers.channel import ChannelSerializer
from channel.models import AuthChannel
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from userprofile.models import UserChannel
from utils.celery.dmp_celery import send_task_channel_general_data_priority
from utils.celery.dmp_celery import send_task_delete_channels
from utils.es_components_api_utils import get_fields
from utils.es_components_cache import flush_cache
from utils.permissions import OnlyAdminUserOrSubscriber
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission

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
        if sections or self.__channel_manager is None:
            self.__channel_manager = ChannelManager(sections)
        return self.__channel_manager

    def put(self, *args, **kwargs):
        data = self.request.data

        if "channel_group" in data and data["channel_group"] not in PERMITTED_CHANNEL_GROUPS:
            return Response(status=HTTP_400_BAD_REQUEST)

        channel_id = kwargs.get("pk")
        try:
            channel = self.channel_manager((Sections.CUSTOM_PROPERTIES, Sections.SOCIAL,)).get([channel_id])
        except NotFoundError:
             return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        if not channel:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        channel = channel[0]
        emails = data.pop("emails") if data.get("emails") else None
        if emails:
            emails = emails.split(",")
            channel.populate_custom_properties(emails=emails)

        # this solution should be used until task to update social section wouldn't be added to DMP
        # only custom_properties section can be updated from restapi
        soical_links = data.pop("social_links") if data.get("social_links") else None
        if soical_links:
            social_data = dict(
                facebook_link=soical_links.get("facebook"),
                twitter_link=soical_links.get("twitter"),
                instagram_link=soical_links.get("instagram")
            )
            channel.populate_social(**social_data)

        channel.populate_custom_properties(**data)

        self.channel_manager().upsert([channel])
        send_task_channel_general_data_priority((channel.main.id,), wait=True)
        flush_cache()
        return self.get(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        if self.request.user.is_staff and self.request.query_params.get("from_youtube") == "1":
            return self.obtain_youtube_statistics()

        channel_id = kwargs.get('pk')
        allowed_sections_to_load = (
            Sections.MAIN, Sections.SOCIAL, Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES,
            Sections.STATS, Sections.ADS_STATS,
            Sections.BRAND_SAFETY,)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))
        if channel_id in user_channels or self.request.user.has_perm("userprofile.channel_audience") \
                or self.request.user.is_staff:
            allowed_sections_to_load += (Sections.ANALYTICS,)

        fields_to_load = get_fields(request.query_params, allowed_sections_to_load)

        try:
            channel = self.channel_manager().model.get(channel_id, _source=fields_to_load)
        except NotFoundError:
             return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        if not channel:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

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

        result = ChannelSerializer(channel).data
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
        flush_cache()
        return Response()

    def get_video_sort_rule(self):
        return [{"general_data.youtube_published_at": {"order": SortDirections.DESCENDING}}]
