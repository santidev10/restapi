from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from singledb.api.views import SingledbApiView
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightVideosListApiView(SingledbApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        return Response(None)
        # request_query_params = request.query_params
        # query_params = HighlightsQuery(request_query_params).prepare_query(mode="video")
        # connector = Connector()
        # try:
        #     response_data = connector.get_highlights_videos(query_params)
        # except SingleDatabaseApiConnectorException as e:
        #     return Response(
        #         data={"error": " ".join(e.args)},
        #         status=HTTP_408_REQUEST_TIMEOUT)
        # VideoListApiView.adapt_response_data(response_data, request.user)
        # response_data = HighlightsQuery.adapt_language_aggregation(response_data)
        # return Response(response_data)
