import re

from copy import deepcopy
from math import ceil

from django.contrib.auth.mixins import PermissionRequiredMixin
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView
from rest_framework_csv.renderers import CSVStreamingRenderer

from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.query_builder import QueryBuilder
from es_components.connections import init_es_connection

from channel.api.mixins import ChannelYoutubeSearchMixin
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.api_views_mixins import SegmentFilterMixin
from utils.api.cassandra_export_mixin import CassandraExportMixinApiView
from utils.brand_safety_view_decorator import add_brand_safety_data

init_es_connection()


DEFAULT_PAGE_NUMBER = 1
DEFAULT_PAGE_SIZE = 50

TERMS_FILTER = ("general_data.country", "general_data.top_language", "analytics.cms_title",
                "general_data.title", "general_data.top_category")

RANGE_FILTER = ("social.instagram_followers", "social.twitter_followers", "social.facebook_likes",
                "stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers")


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


class ChannelListCSVRendered(CSVStreamingRenderer):
    header = [
        "title",
        "url",
        "country",
        "category",
        "emails",
        "subscribers",
        "thirty_days_subscribers",
        "thirty_days_views",
        "views_per_video",
        "sentiment",
        "engage_rate",
        "last_video_published_at",
        "brand_safety_score",
        "video_view_rate",
        "ctr",
        "ctr_v",
        "average_cpv"
    ]


class UserChannelsNotAvailable(Exception):
    pass


class ChannelListApiView(APIView, CassandraExportMixinApiView, PermissionRequiredMixin, ChannelYoutubeSearchMixin,
                         SegmentFilterMixin):
    """
    Proxy view for channel list
    """
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    renderer = ChannelListCSVRendered
    export_file_title = "channel"

    empty_response = {
        "max_page": 1,
        "items_count": 0,
        "items": [],
        "current_page": 1,
    }
    max_pages_count = 200
    es_manager = ChannelManager

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
        if request.user.is_staff and any((
                request.query_params.get("youtube_link"),
                request.query_params.get("youtube_keyword"))):
            return self.search_channels()

        allowed_sections_to_load = (Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS)
        query_params = deepcopy(request.query_params)

        try:
            channels_ids = self.get_own_channel_ids(request.user, query_params)
        except UserChannelsNotAvailable:
            return Response(self.empty_response)

        if request.user.is_staff or channels_ids:
            allowed_sections_to_load += (Sections.ANALYTICS,)

        es_manager = self.es_manager(allowed_sections_to_load)

        filters = QueryGenerator(query_params).get_search_filters(channels_ids)
        sort = self.get_sort_rule(query_params)
        size, offset, page = self.get_limits(query_params)

        try:
            items_count = es_manager.search(filters=filters, sort=sort, limit=None).count()
            channels = es_manager.search(filters=filters, sort=sort, limit=size, offset=offset).execute().hits

            aggregations = es_manager.get_aggregation(es_manager.search(filters=filters, limit=None)) \
                if query_params.get("aggregations") else None

        except Exception as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        max_page = None
        if size:
            max_page = min(ceil(items_count / size), self.max_pages_count)

        result = {
            "current_page": page,
            "items": [channel.to_dict() for channel in channels],
            "items_count": items_count,
            "max_page": max_page,
            "aggregations": aggregations.to_dict() if aggregations else None
        }
        return Response(result)

    @staticmethod
    def get_own_channel_ids(user, query_params):
        own_channels = int(query_params.get("own_channels", "0"))
        user_can_see_own_channels = user.has_perm("userprofile.settings_my_yt_channels")

        if own_channels and not user_can_see_own_channels:
            raise UserChannelsNotAvailable

        if own_channels and user_can_see_own_channels:
            channels_ids = list(user.channels.values_list("channel_id", flat=True))

            if not channels_ids:
                raise UserChannelsNotAvailable

            return channels_ids

    def get_limits(self, query_params):
        size = int(query_params.get("size", [DEFAULT_PAGE_SIZE])[0])
        page = query_params.get("page", [DEFAULT_PAGE_NUMBER])
        if len(page) > 1:
            raise ValueError("Passed more than one page number")

        page = int(page[0])
        offset = 0 if page <= 1 else (page - 1) * size

        return size, offset, page

    def get_sort_rule(self, query_params):
        sort_params = query_params.get("sort", None)

        if sort_params:
            key, direction = sort_params.split(":")
            return [{key: {"order": direction}}]

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
        return Connector().get_channel_list_full(filters, fields=ChannelListCSVRendered.header, batch_size=1000)


class QueryGenerator:
    es_manager = ChannelManager()
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER

    def __init__(self, query_params):
        self.query_params = query_params

    def __get_filter_range(self):
        filters = []

        for field in self.range_filter:

            range = self.query_params.get(field, None)

            if range:
                min, max = range.split(",")

                if not (min and max):
                    continue

                query = QueryBuilder().build().must().range().field(field)
                if min:
                    query = query.gte(int(min))
                if max:
                    query = query.lte(int(max))
                filters.append(query.get())

        return filters

    def __get_filters_term(self):
        filters = []

        for field in self.terms_filter:

            value = self.query_params.get(field, None)
            if value:
                value = value.split(",")
                filters.append(
                    QueryBuilder().build().must().terms().field(field).value(value).get()
                )

        return filters

    def get_search_filters(self, channels_ids=None):
        filters_term = self.__get_filters_term()
        filters_range = self.__get_filter_range()
        forced_filter = [self.es_manager.forced_filters()]

        filters = filters_term + filters_range + forced_filter

        if channels_ids:
            filters.append(self.es_manager.ids_query(channels_ids))

        return filters
