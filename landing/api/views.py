"""
Feedback api views module
"""
from django.conf import settings
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from es_components.managers.channel import ChannelManager
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.query_builder import QueryBuilder

from channel.models import AuthChannel
from utils.utils import chunks_generator

CHANNEL_SUBSCRIBERS_FIELD = "stats.subscribers"
CHUNK_SIZE = 10000


class TopAuthChannels(APIView):
    permission_classes = tuple()
    manager = ChannelManager((Sections.GENERAL_DATA, Sections.STATS))
    subscribers_min_value = 10000
    top_last_auth_items_limit = 21

    def get_top_last_auth(self):
        remaining_len = self.top_last_auth_items_limit
        top_auth_channels = []

        for auth_channels in chunks_generator(AuthChannel.objects.all().order_by("-created_at"), CHUNK_SIZE):

            if remaining_len <= 0:
                break

            subscribers_range_filter = QueryBuilder().build().must().range()\
                .field(CHANNEL_SUBSCRIBERS_FIELD).gte(self.subscribers_min_value).get()

            ids_filter = self.manager.ids_query([channel.channel_id for channel in auth_channels])
            filters = self.manager.forced_filters() & subscribers_range_filter & ids_filter

            channels = self.manager.search(filters=filters, limit=remaining_len).execute().hits
            remaining_len -= len(channels)
            top_auth_channels += channels

        return top_auth_channels

    def get_testimonials(self):
        ids_filter = self.manager.ids_query(list(settings.TESTIMONIALS.keys()))
        sort_rule = [{CHANNEL_SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}]
        testimonials_channels = self.manager.search(filters=ids_filter, sort=sort_rule).execute().hits
        return testimonials_channels

    def __add_video_id(self, channel):
        channel_id = channel["main"]["id"]
        if channel_id in settings.TESTIMONIALS:
            video_id = settings.TESTIMONIALS[channel_id]
            channel["video_id"] = video_id
        return channel

    def get(self, request):
        try:
            channels_last_authed = self.get_top_last_auth()
            channels_testimonials = self.get_testimonials()
        except Exception as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        data = {
            "last": [channel.to_dict() for channel in channels_last_authed],
            "testimonials": [self.__add_video_id(channel.to_dict()) for channel in channels_testimonials]
        }

        return Response(data)
