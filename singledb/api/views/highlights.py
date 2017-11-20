from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT

from channel.api.views import ChannelListApiView
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from video.api.views import VideoListApiView


class HighlightChannelsListApiView(SingledbApiView):
    def get(self, request, *args, **kwargs):
        request_query_params = request.query_params
        query_params = HighlightsQuery(request_query_params).prepare_query()
        connector = Connector()
        try:
            response_data = connector.get_highlights_channels(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        ChannelListApiView.adapt_response_data(response_data)
        return Response(response_data)


class HighlightVideosListApiView(SingledbApiView):
    def get(self, request, *args, **kwargs):
        request_query_params = request.query_params
        query_params = HighlightsQuery(request_query_params).prepare_query()
        connector = Connector()
        try:
            response_data = connector.get_highlights_videos(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        VideoListApiView.adapt_response_data(response_data)
        return Response(response_data)


class HighlightsQuery:
    allowed_filters = ('category__terms',)
    allowed_sorts = ('thirty_days_subscribers', 'thirty_days_views', 'thirty_days_comments', 'thirty_days_likes',
                     'weekly_subscribers', 'weekly_views', 'weekly_comments', 'weekly_likes',
                     'daily_subscribers', 'daily_views', 'daily_comments', 'daily_likes',)
    allowed_sorts_type = ('desc',)
    allowed_aggregations = ('category',)

    def __init__(self, query_params):
        self.result_query_params = CustomQueryParamsDict()
        self.result_query_params._mutable = True
        self.request_query_params = query_params

    def prepare_query(self):
        size = self.request_query_params.get('size')
        if size:
            self.result_query_params['size'] = 20 if int(size) > 20 else size
        else:
            self.result_query_params['size'] = 20

        if self.request_query_params.get('sort'):
            sort, sort_type = self.request_query_params.get('sort').split(':', 1)
            if sort in self.allowed_sorts \
                    and sort_type in self.allowed_sorts_type:
                self.result_query_params['sort'] = self.request_query_params.get('sort')

        for allowed_filter in self.allowed_filters:
            if self.request_query_params.get(allowed_filter):
                self.result_query_params[allowed_filter] = self.request_query_params.get(allowed_filter)

        if self.request_query_params.get('aggregations') in self.allowed_aggregations:
            self.result_query_params['aggregations'] = self.request_query_params.get('aggregations')

        return self.result_query_params


class CustomQueryParamsDict(dict):
    pass
