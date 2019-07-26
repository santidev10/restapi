from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from channel.api.views import ChannelListApiView
from channel.api.views.channel_list import BaseChannelListApiView
from highlights.api.views.highlights_query import HighlightsQuery
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightChannelsListApiView(APIView,
                                   BaseChannelListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )

    max_pages_count = 5

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        response_data = self._get_channel_list_data(request)
        return Response(response_data)
        # request_query_params = request.query_params
        # query_params = HighlightsQuery(request_query_params).prepare_query(mode="channel")
        # connector = Connector()
        # try:
        #     response_data = connector.get_highlights_channels(query_params)
        # except SingleDatabaseApiConnectorException as e:
        #     return Response(
        #         data={"error": " ".join(e.args)},
        #         status=HTTP_408_REQUEST_TIMEOUT)
        # ChannelListApiView.adapt_response_data(response_data, request.user)
        # response_data = HighlightsQuery.adapt_language_aggregation(response_data)
        # return Response(response_data)
