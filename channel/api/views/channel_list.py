import re
from copy import deepcopy

from django.contrib.auth.mixins import PermissionRequiredMixin
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from channel.api.mixins import ChannelYoutubeSearchMixin
from segment.models import SegmentVideo
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.api_views_mixins import SegmentFilterMixin
from utils.csv_export import CassandraExportMixin
from utils.brand_safety_view_decorator import add_brand_safety_data

CHANNEL_ITEM_SCHEMA = openapi.Schema(
    title="Youtube channel",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        description=openapi.Schema(type=openapi.TYPE_STRING),
        id=openapi.Schema(type=openapi.TYPE_STRING),
        subscribers=openapi.Schema(type=openapi.TYPE_STRING),
        thumbnail_image_url=openapi.Schema(type=openapi.TYPE_STRING),
        title=openapi.Schema(type=openapi.TYPE_STRING),
        videos=openapi.Schema(type=openapi.TYPE_STRING),
        views=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)
CHANNELS_SEARCH_RESPONSE_SCHEMA = openapi.Schema(
    title="Youtube channel paginated response",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        max_page=openapi.Schema(type=openapi.TYPE_INTEGER),
        items_count=openapi.Schema(type=openapi.TYPE_INTEGER),
        current_page=openapi.Schema(type=openapi.TYPE_INTEGER),
        items=openapi.Schema(
            title="Youtube channel list",
            type=openapi.TYPE_ARRAY,
            items=CHANNEL_ITEM_SCHEMA,
        ),
    ),
)


class ChannelListApiView(APIView, PermissionRequiredMixin, CassandraExportMixin, ChannelYoutubeSearchMixin,
                         SegmentFilterMixin):
    """
    Proxy view for channel list
    """
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
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
        "engage_rate",
        "last_video_published_at"
    ]
    export_file_title = "channel"

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="youtube_link",
                required=False,
                in_=openapi.IN_QUERY,
                description="Youtube channel URL",
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                name="youtube_keyword",
                required=False,
                in_=openapi.IN_QUERY,
                description="Search string to find Youtube channela",
                type=openapi.TYPE_STRING,
            )
        ],
        responses={
            HTTP_200_OK: CHANNELS_SEARCH_RESPONSE_SCHEMA,
            HTTP_400_BAD_REQUEST: openapi.Response("Wrong request parameters"),
            HTTP_404_NOT_FOUND: openapi.Response("Channel not found"),
            HTTP_408_REQUEST_TIMEOUT: openapi.Response("Request timeout"),
        }
    )
    @add_brand_safety_data
    def get(self, request):
        """
        Get procedure
        """
        # search procedure
        if request.user.is_staff and any((
                request.query_params.get("youtube_link"),
                request.query_params.get("youtube_keyword"))):
            return self.search_channels()
        # query params validation
        is_query_params_valid, error = self._validate_query_params()
        if not is_query_params_valid:
            return Response({"error": error}, HTTP_400_BAD_REQUEST)
        # init procedures
        empty_response = {
            "max_page": 1,
            "items_count": 0,
            "items": [],
            "current_page": 1,
        }
        # prepare query params
        query_params = deepcopy(request.query_params)
        query_params._mutable = True
        channels_ids = []
        connector = Connector()
        # own channels
        user = request.user
        own_channels = query_params.get("own_channels", "0")
        user_can_see_own_channels = user.has_perm(
            "userprofile.settings_my_yt_channels")
        if own_channels == "1" and user_can_see_own_channels:
            channels_ids = list(
                user.channels.values_list("channel_id", flat=True))
            if not channels_ids:
                return Response(empty_response)
            try:
                ids_hash = connector.store_ids(list(channels_ids))
            except SingleDatabaseApiConnectorException as e:
                return Response(data={"error": " ".join(e.args)},
                                status=HTTP_408_REQUEST_TIMEOUT)
            query_params.update(ids_hash=ids_hash)
        elif own_channels == "1" and not user_can_see_own_channels:
            return Response(empty_response)
        channel_segment_id = self.request.query_params.get("channel_segment")
        video_segment_id = self.request.query_params.get("video_segment")
        if any((channel_segment_id, video_segment_id)):
            segment = self._obtain_segment()
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            if isinstance(segment, SegmentVideo):
                segment_videos_ids = segment.get_related_ids()
                try:
                    ids_hash = connector.store_ids(list(segment_videos_ids))
                except SingleDatabaseApiConnectorException as e:
                    return Response(data={"error": " ".join(e.args)},
                                    status=HTTP_408_REQUEST_TIMEOUT)
                request_params = {
                    "ids_hash": ids_hash,
                    "fields": "channel_id",
                    "size": 10000
                }
                try:
                    videos_data = connector.get_video_list(request_params)
                except SingleDatabaseApiConnectorException as e:
                    return Response(data={"error": " ".join(e.args)},
                                    status=HTTP_408_REQUEST_TIMEOUT)
                segment_channels_ids = {
                    obj.get("channel_id") for obj in videos_data.get("items")}
                query_params.pop("video_segment")
            else:
                segment_channels_ids = segment.get_related_ids()
                query_params.pop("channel_segment")
            if channels_ids:
                channels_ids = [
                    channel_id
                    for channel_id in channels_ids
                    if channel_id in segment_channels_ids]
            else:
                channels_ids = segment_channels_ids
            if not channels_ids:
                return Response(empty_response)
            try:
                ids_hash = connector.store_ids(list(channels_ids))
            except SingleDatabaseApiConnectorException as e:
                return Response(data={"error": " ".join(e.args)},
                                status=HTTP_408_REQUEST_TIMEOUT)
            query_params.update(ids_hash=ids_hash)
        # adapt the request params
        self.adapt_query_params(query_params)
        # make call
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        # adapt the response data
        self.adapt_response_data(response_data, request.user)
        return Response(response_data)

    @staticmethod
    def adapt_query_params(query_params):
        """
        Adapt SDB request format
        """

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
            _range = [str(v) if v is not None else "" for v in _range]
            _range = ",".join(_range)
            if _range != ",":
                query_params.update(**{"{}__range".format(name): _range})

        def make(_type, name, name_in=None):
            if name_in is None:
                name_in = name
            value = query_params.pop(name_in, [None])[0]
            if value is not None:
                query_params.update(**{"{}__{}".format(name, _type): value})

        # min_subscribers_yt, max_subscribers_yt
        make_range("subscribers", "min_subscribers_yt", "max_subscribers_yt")

        # country
        make("terms", "country")

        # language
        make("terms", "language")

        # min_thirty_days_subscribers, max_thirty_days_subscribers
        make_range("thirty_days_subscribers")

        # min_thirty_days_views, max_thirty_days_views
        make_range("thirty_days_views")

        # min_sentiment, max_sentiment
        make_range("sentiment")

        # min_engage_rate, max_engage_rate
        make_range("engage_rate")

        # min_views_per_video, max_views_per_video
        make_range("views_per_video")

        # min_subscribers_fb, max_subscribers_fb
        make_range(
            "facebook_likes", "min_subscribers_fb", "max_subscribers_fb")

        # min_subscribers_tw, max_subscribers_tw
        make_range(
            "twitter_followers", "min_subscribers_tw", "max_subscribers_tw")

        # min_subscribers_in, max_subscribers_in
        make_range(
            "instagram_followers", "min_subscribers_in", "max_subscribers_in")

        # category
        category = query_params.pop("category", [None])[0]
        if category is not None:
            regexp = "|".join([".*" + c + ".*" for c in category.split(",")])
            query_params.update(category__regexp=regexp)

        # text_search
        text_search = query_params.pop("text_search", [None])[0]
        if text_search:
            query_params.update(text_search__match_phrase=text_search)

        # channel_group
        make("term", "channel_group")
        # <--- filters

    @staticmethod
    def adapt_response_data(response_data, user):
        """
        Adapt SDB response format
        """
        user_channels = set(user.channels.values_list(
            "channel_id", flat=True))
        items = response_data.get("items", [])
        for item in items:
            if "channel_id" in item:
                item["id"] = item.get("channel_id", "")
                item["is_owner"] = item["channel_id"] in user_channels
                del item["channel_id"]
            if "country" in item and item["country"] is None:
                item["country"] = ""
            if "history_date" in item and item["history_date"]:
                item["history_date"] = item["history_date"][:10]

            is_own = item.get("is_owner", False)
            if user.has_perm('userprofile.channel_audience') \
                    or is_own:
                pass
            else:
                item['has_audience'] = False
                item["verified"] = False
                item.pop('audience', None)
                item['brand_safety'] = None
                item['safety_chart_data'] = None
                item.pop('traffic_sources', None)

            if not user.is_staff:
                item.pop("cms__title", None)

            for field in ["youtube_published_at", "updated_at"]:
                if field in item and item[field]:
                    item[field] = re.sub(
                        "^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+|)$",
                        "\g<0>Z",
                        item[field]
                    )
        return response_data

    def _data_filtered_batch_generator(self, filters):
        return Connector().get_channel_list_full(filters, fields=self.fields_to_export, batch_size=1000)
