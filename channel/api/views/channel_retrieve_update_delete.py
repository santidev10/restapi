from datetime import datetime

from dateutil import parser
from django.http import QueryDict
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from channel.api.mixins import ChannelYoutubeStatisticsMixin
from channel.api.views.channel_list import ChannelListApiView
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from userprofile.models import UserChannel
from utils.permissions import OnlyAdminUserOrSubscriber
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class OwnChannelPermissions(BasePermission):
    def has_permission(self, request, view):
        return UserChannel.objects \
            .filter(user=request.user, channel_id=view.kwargs.get("pk")) \
            .exists()


class ChannelRetrieveUpdateDeleteApiView(SingledbApiView, ChannelYoutubeStatisticsMixin):
    permission_classes = (
        or_permission_classes(
            user_has_permission('userprofile.channel_details'),
            OwnChannelPermissions,
            OnlyAdminUserOrSubscriber),)
    _connector_get = None
    _connector_put = None

    @property
    def connector_put(self):
        """
        Lazy loaded property.
        Purpose: allows patching it in tests
        """
        if self._connector_put is None:
            self._connector_put = Connector().put_channel
        return self._connector_put

    @property
    def connector_get(self):
        """
        Lazy loaded property.
        Purpose: allows patching it in tests
        """
        if self._connector_get is None:
            self._connector_get = Connector().get_channel
        return self._connector_get

    def put(self, *args, **kwargs):
        data = self.request.data
        permitted_groups = ["influencers", "new", "media", "brands"]
        if "channel_group" in data and data[
            "channel_group"] not in permitted_groups:
            return Response(status=HTTP_400_BAD_REQUEST)
        response = super().put(*args, **kwargs)
        ChannelListApiView.adapt_response_data(
            {'items': [response.data]}, self.request.user)
        return response

    def get(self, *args, **kwargs):
        if self.request.user.is_staff and \
                self.request.query_params.get("from_youtube") == "1":
            return self.obtain_youtube_statistics()
        response = super().get(*args, **kwargs)
        pk = kwargs.get('pk')
        if pk:
            query = QueryDict("channel_id__term={}"
                              "&sort=youtube_published_at:desc"
                              "&size=50"
                              "&fields=video_id"
                              ",title"
                              ",thumbnail_image_url"
                              ",views"
                              ",youtube_published_at"
                              ",likes"
                              ",comments".format(pk))
            videos = Connector().get_video_list(query)['items']
            now = datetime.now()
            average_views = 0
            if len(videos):
                average_views = round(
                    sum([v.get("views", 0) for v in videos]) / len(videos))
            for v in videos:
                v["id"] = v.pop("video_id")
                youtube_published_at = v.pop("youtube_published_at", None)
                if youtube_published_at:
                    v['days'] = (now - parser.parse(youtube_published_at)).days
            response.data["performance"] = {
                'average_views': average_views,
                'videos': videos,
            }

        ChannelListApiView.adapt_response_data(
            {'items': [response.data]}, self.request.user)
        return response

    def delete(self, *args, **kwargs):
        pk = kwargs.get('pk')
        UserChannel.objects \
            .filter(channel_id=pk, user=self.request.user) \
            .delete()
        if not UserChannel.objects.filter(channel_id=pk).exists():
            Connector().unauthorize_channel(pk)
        return Response()
