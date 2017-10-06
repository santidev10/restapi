"""
Channel api views module
"""
from copy import deepcopy
import re

from django.db.models import Q
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT, HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from segment.models import SegmentChannel
# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from utils.csv_export import list_export
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete, \
    OnlyAdminUserOrSubscriber


# pylint: enable=import-error


class ChannelListApiView(APIView):
    """
    Proxy view for channel list
    """
    # TODO Check additional auth logic
    permission_classes = (OnlyAdminUserOrSubscriber,)
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

        # adapt the request params
        self.adapt_query_params(query_params)

        # make call
        connector = Connector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)

        # adapt the response data
        self.adapt_response_data(response_data)

        return Response(response_data)

    @staticmethod
    def adapt_query_params(query_params):
        """
        Adapt SDB request format
        """
        # sorting --->
        sorting = query_params.pop("sort_by", ["subscribers"])[0]
        if sorting in ["subscribers", "sentiment", "views_per_video", "thirty_days_views", "thirty_days_subscribers", "score_total"]:
            query_params.update(sort="{}:desc".format(sorting))
        elif sorting == "engagement":
            query_params.update(sort="engage_rate:desc")
        else:
            query_params.update(sort="subscribers:desc")
        # <--- sorting

        # filters --->
        def make_range(name, name_min=None, name_max=None):
            if name_min is None:
                name_min = "min_{}".format(name)
            if name_max is None:
                name_max = "max_{}".format(name)
            _range = [
                query_params.pop(name_min, [None])[0],
                query_params.pop(name_max, [None])[0],
            ]
            _range = [str(v) if v is not None else '' for v in _range]
            _range = ','.join(_range)
            if _range != ',':
                query_params.update(**{"{}__range".format(name): _range})

        def make(_type, name, name_in=None):
            if name_in is None:
                name_in = name
            value = query_params.pop(name_in, [None])[0]
            if value is not None:
                query_params.update(**{"{}__{}".format(name, _type): value})

        # min_subscribers_yt, max_subscribers_yt
        make_range('subscribers', 'min_subscribers_yt', 'max_subscribers_yt')

        # country
        make('terms', 'country')

        # language
        make('terms', 'language')

        # min_thirty_days_subscribers, max_thirty_days_subscribers
        make_range('thirty_days_subscribers')

        # min_thirty_days_views, max_thirty_days_views
        make_range('thirty_days_views')

        # min_sentiment, max_sentiment
        make_range('sentiment')

        # min_engage_rate, max_engage_rate
        make_range('engage_rate')

        # min_views_per_video, max_views_per_video
        make_range('views_per_video')

        # min_subscribers_fb, max_subscribers_fb
        make_range('facebook_likes', 'min_subscribers_fb', 'max_subscribers_fb')

        # min_subscribers_tw, max_subscribers_tw
        make_range('twitter_followers', 'min_subscribers_tw', 'max_subscribers_tw')

        # min_subscribers_in, max_subscribers_in
        make_range('instagram_followers', 'min_subscribers_in', 'max_subscribers_in')

        # category
        category = query_params.pop('category', [None])[0]
        if category is not None:
            regexp = '|'.join(['.*' + c + '.*' for c in category.split(',')])
            query_params.update(category__regexp=regexp)

        # search
        make('term', 'text_search', 'search')
        # <--- filters

    @staticmethod
    def adapt_response_data(response_data):
        """
        Adapt SDB response format
        """
        items = response_data.get("items", [])
        for item in items:
            if "channel_id" in item: 
                item["id"] = item.get("channel_id", "")
                del item["channel_id"]
            if 'country' in item and item['country'] is None:
                item['country'] = ""
            if "history_date" in item:
                item["history_date"] = item["history_date"][:10]
            if "youtube_published_at" in item:
                item["youtube_published_at"] = re.sub(
                    "^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",
                    "\g<0>Z",
                    item["youtube_published_at"]
                )
        return response_data


class ChannelListFiltersApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber,)

    connector_get = Connector().get_channel_filters_list


class ChannelRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_get = Connector().get_channel
    connector_put = Connector().put_channel

    def put(self, *args, **kwargs):
        if "channel_group" in self.request.data \
            and self.request.data["channel_group"] not in ["influencers","new","media","brands"]:
                return Response(status=HTTP_400_BAD_REQUEST)
        return super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        response = super().get(*args, **kwargs)
        ChannelListApiView.adapt_response_data({'items': [response.data]})
        return response


class ChannelSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_channels


class ChannelsVideosByKeywords(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber,)

    def get(self, request, *args, **kwargs):
        keyword = kwargs.get("keyword")
        query_params = request.query_params
        connector = Connector()
        try:
            response_data = connector.get_channel_videos_by_keywords(query_params, keyword)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)
