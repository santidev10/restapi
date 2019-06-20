from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT

from channel.api.views import ChannelListApiView
from segment.api.mixins import DynamicModelViewMixin
from segment.api.serializers import SegmentSerializer
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException


class SegmentSuggestedChannelApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer
    connector = Connector()

    def get(self, request, *args, **kwargs):
        segment = self.get_object()
        query_params = self.request.query_params
        query_params._mutable = True
        response_data = []

        if segment.top_recommend_channels:
            try:
                query_params['ids'] = ','.join(
                    segment.top_recommend_channels[:100])
                response_data = self.connector.get_channel_list(query_params)
            except SingleDatabaseApiConnectorException:
                return Response(status=HTTP_408_REQUEST_TIMEOUT)
        if response_data:
            ChannelListApiView.adapt_response_data(response_data, request.user)
        return Response(response_data)
