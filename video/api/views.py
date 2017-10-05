"""
Video api views module
"""
from copy import deepcopy
from datetime import timedelta
from dateutil.parser import parse
import re

from django.db.models import Q
from django.utils import timezone
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from segment.models import SegmentVideo
# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from singledb.settings import DEFAULT_VIDEO_LIST_FIELDS, \
    DEFAULT_VIDEO_DETAILS_FIELDS
# pylint: enable=import-error
from utils.csv_export import list_export
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete, \
    OnlyAdminUserOrSubscriber


class VideoListApiView(APIView):
    """
    Proxy view for video list
    """
    # TODO Check additional auth logic
    permission_classes = (OnlyAdminUserOrSubscriber,)
    fields_to_export = [
        "title",
        "url",
        "views",
        "likes",
        "dislikes",
        "comments",
        "youtube_published_at"
    ]
    export_file_title = "video"
    default_request_fields = DEFAULT_VIDEO_LIST_FIELDS

    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = SegmentVideo.objects.get(id=segment_id)
            else:
                segment = SegmentVideo.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except SegmentVideo.DoesNotExist:
            return None
        return segment

    @list_export
    def get(self, request):
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
            videos_ids = segment.get_related_ids()
            if not videos_ids:
                empty_response = {
                    "max_page": 1,
                    "items_count": 0,
                    "items": [],
                    "current_page": 1,
                }
                return Response(empty_response)

            query_params.pop("segment")
            query_params.update(ids=",".join(videos_ids))

        # adapt the request params
        self.adapt_query_params(query_params)

        # make call
        connector = Connector()
        try:
            response_data = connector.get_video_list(query_params)
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
        sorting = query_params.pop("sort_by", ["views"])[0]
        if sorting in ["views", "likes", "dislikes", "comments", "sentiment"]:
            query_params.update(sort='{}:desc'.format(sorting))
        elif sorting == 'engagement':
            query_params.update(sort='engage_rate:desc')
        else:
            query_params.update(sort='views:desc')
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

        # min_views, max_views
        make_range('views')

        # min_daily_views, max_daily_views
        make_range('daily_views')

        # min_sentiment, max_sentiment
        make_range('sentiment')

        # min_engage_rate, max_engage_rate
        make_range('engage_rate')

        # min_subscribers, max_subscribers
        make_range('channel__subscribers', 'min_subscribers', 'max_subscribers')

        # country
        make('terms', 'country')

        # category
        make('terms', 'category')

        # language
        make('terms', 'lang_code', 'language')

        # search
        make('term', 'text_search', 'search')

        # channel
        make('terms', 'channel_id', 'channel')

        # brand_safety
        brand_safety = query_params.pop('brand_safety', [None])[0]
        if brand_safety is not None:
            val = "true" if brand_safety == "1" else "false"
            query_params.update(has_transcript__term=val)

        # upload_at
        upload_at = query_params.pop('upload_at', [None])[0]
        if upload_at is not None:
            if upload_at != "0":
                try:
                    date = parse(upload_at).date()
                except (TypeError, ValueError):
                    pass
                query_params.update(youtube_published_at__range="{},".format(date.isoformat()))
            elif upload_at == "0":
                now = timezone.now()
                start = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
                end = start + timedelta(hours=23, minutes=59, seconds=59, microseconds=999999)
                query_params.update(youtube_published_at__range="{},{}".format(start.isoformat(), end.isoformat()))

        # trending
        trending = query_params.pop('trending', [None])[0]
        if trending is not None and trending != 'all':
            query_params.update(trends_list__term=trending)
        # <--- filters

    @staticmethod
    def adapt_response_data(response_data):
        """
        Adapt SDB response format
        """
        items = response_data.get('items', [])
        for item in items:
            if 'video_id' in item:
                item['id'] = item.get('video_id', "")
                del item['video_id']

            if 'history_date' in item:
                item['history_date'] = item['history_date'][:10]

            if 'country' in item and item['country'] is None:
                item['country'] = ""

            if 'youtube_published_at' in item:
                item['youtube_published_at'] = re.sub('^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
                                                      '\g<0>Z',
                                                      item['youtube_published_at'])
        return response_data



class VideoListFiltersApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber,)

    connector_get = Connector().get_video_filters_list


class VideoRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_get = Connector().get_video
    connector_put = Connector().put_video
    default_request_fields = DEFAULT_VIDEO_DETAILS_FIELDS


class VideoSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_videos
