from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT

from channel.api.views import ChannelListApiView
from highlights.api.views.highlights_query import HighlightsQuery
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.brand_safety_view_decorator import add_brand_safety_data


class HighlightChannelsListApiView(SingledbApiView):
    permission_required = (
        "userprofile.view_highlights",
    )

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        request_query_params = request.query_params
        query_params = HighlightsQuery(request_query_params).prepare_query(mode="channel")
        connector = Connector()
        try:
            response_data = connector.get_highlights_channels(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        ChannelListApiView.adapt_response_data(response_data, request.user)
        response_data = HighlightsQuery.adapt_language_aggregation(response_data)
        return Response(response_data)
