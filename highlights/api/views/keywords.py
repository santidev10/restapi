from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT

from highlights.api.views.highlights_query import HighlightsQuery
from keywords.api import KeywordListApiView
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException


class HighlightKeywordsListApiView(SingledbApiView):
    permission_required = (
        "userprofile.view_highlights",
    )

    def get(self, request, *args, **kwargs):
        request_query_params = request.query_params
        query_params = HighlightsQuery(request_query_params).prepare_query(mode='keyword')
        connector = Connector()
        try:
            response_data = connector.get_highlights_keywords(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        KeywordListApiView.adapt_response_data(request=self.request,
                                               response_data=response_data)
        return Response(response_data)
