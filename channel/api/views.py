"""
Channel api views module
"""
from django.db.models import Q
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT, HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from segment.models import Segment
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException


class ChannelListApiView(APIView):
    """
    Proxy view for channel list
    """
    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = Segment.objects.get(id=segment_id)
            else:
                segment = Segment.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except Segment.DoesNotExist:
            return None
        return segment

    def get(self, request):
        """
        Get procedure
        """
        # prepare query params
        query_params = request.query_params
        query_params._mutable = True
        # segment
        segment = query_params.get("segment")
        if segment is not None:
            # obtain segment
            segment = self.obtain_segment(segment)
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            # obtain channels ids
            channels_ids = segment.channels.values_list(
                "channel_id", flat=True)
            query_params.pop("segment")
            query_params.update(ids=",".join(channels_ids))
            # TODO we can't be sure that all segment channels are still in SDB
        # make call
        connector = SingleDatabaseApiConnector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)


class ChannelListFiltersApiView(SingledbApiView):
    connector_get = Connector().get_channel_filters_list


class ChannelRetrieveUpdateApiView(SingledbApiView):
    connector_get = Connector().get_channel
    connector_put = Connector().put_channel


class ChannelSetApiView(SingledbApiView):
    connector_delete = Connector().delete_channels
