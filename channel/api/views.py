"""
Channel api views module
"""
from copy import deepcopy
import re

from django.db.models import Q
from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT, HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_412_PRECONDITION_FAILED

from rest_framework.views import APIView

from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from segment.models import SegmentChannel
from userprofile.models import UserChannel

# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from utils.csv_export import list_export
# pylint: enable=import-error


class ChannelListApiView(APIView, PermissionRequiredMixin):
    """
    Proxy view for channel list
    """
    permission_required = ('userprofile.channel_list',)

    fields_to_export = [
        "title",
        "url",
        "country",
        "category",
        "emails",
        "description",
        "subscribers",
        "thirty_days_subscribers",
        "thirty_days_views",
        "views_per_video",
        "sentiment",
        "engage_rate"
    ]
    export_file_title = "channel"

    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = SegmentChannel.objects.get(id=segment_id)
            else:
                segment = SegmentChannel.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except SegmentChannel.DoesNotExist:
            return None
        return segment

    @list_export
    def get(self, request):
        """
        Get procedure
        """
        # prepare query params
        query_params = deepcopy(request.query_params)
        query_params._mutable = True
        # segment
        segment = query_params.get("segment")
        if segment is not None:
            # obtain segment
            segment = self.obtain_segment(segment)
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            # obtain channels ids
            channels_ids = segment.get_related_ids()
            if not channels_ids:
                empty_response = {
                    "max_page": 1,
                    "items_count": 0,
                    "items": [],
                    "current_page": 1,
                }
                return Response(empty_response)
            query_params.pop("segment")
            query_params.update(ids=",".join(channels_ids))

        # own_channels
        own_channels = query_params.get("own_channels", "0")
        if own_channels == "1":
            user = self.request.user
            if not user or not user.is_authenticated():
                return Response(status=HTTP_412_PRECONDITION_FAILED)
            channels_ids = user.channels.values_list('channel_id', flat=True)
            if not channels_ids:
                empty_response = {
                    "max_page": 1,
                    "items_count": 0,
                    "items": [],
                    "current_page": 1,
                }
                return Response(empty_response)
            query_params.pop("own_channels")
            query_params.update(ids=",".join(channels_ids))

        # make call
        connector = Connector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)

        return Response(response_data)


class ChannelListFiltersApiView(SingledbApiView):
    permission_required = tuple() # ('userprofile.channel_filter',)
    connector_get = Connector().get_channel_filters_list


class ChannelRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    permission_required = ('userprofile.channel_details',)
    connector_get = Connector().get_channel
    connector_put = Connector().put_channel

    def put(self, *args, **kwargs):
        if 'channel_group' in self.request.data \
           and self.request.data['channel_group'] not in ['influencers', 'new', 'media', 'brands']:
                return Response(status=HTTP_400_BAD_REQUEST)
        return super().put(*args, **kwargs)


class ChannelSetApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_channels


class ChannelsVideosByKeywords(SingledbApiView):
    def get(self, request, *args, **kwargs):
        keyword = kwargs.get('keyword')
        query_params = request.query_params
        connector = Connector()
        try:
            response_data = connector.get_channel_videos_by_keywords(query_params, keyword)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)
