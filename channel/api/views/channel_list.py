import re

from copy import deepcopy
from math import ceil
from datetime import datetime
from datetime import timedelta

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
from utils.es_components_api_utils import get_limits
from utils.es_components_api_utils import get_sort_rule
from utils.es_components_api_utils import QueryGenerator

init_es_connection()


TERMS_FILTER = ("general_data.country", "general_data.top_language", "general_data.top_category",
                "custom_properties.preferred", "analytics.verified", "analytics.cms_title")

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("social.instagram_followers", "social.twitter_followers", "social.facebook_likes",
                "stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_")

EXISTS_FILTER = ("custom_properties.emails", "ads_stats")


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

        allowed_sections_to_load = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL)
        query_params = deepcopy(request.query_params)

        try:
            channels_ids = self.get_own_channel_ids(request.user, query_params)
        except UserChannelsNotAvailable:
            return Response(self.empty_response)

        if request.user.is_staff or channels_ids:
            allowed_sections_to_load += (Sections.ANALYTICS,)

        es_manager = self.es_manager()

        filters = ChannelQueryGenerator(query_params).get_search_filters(channels_ids)
        sort = get_sort_rule(query_params)
        size, offset, page = get_limits(query_params)

        fields_to_load = self.get_fields(query_params, allowed_sections_to_load)

        try:
            items_count = es_manager.search(filters=filters, sort=sort, limit=None).count()
            channels = es_manager.search(filters=filters, sort=sort, limit=size + offset, offset=offset)\
                .source(includes=fields_to_load).execute().hits

            aggregations = es_manager.get_aggregation(es_manager.search(filters=filters, limit=None)) \
                if query_params.get("aggregations") else None

        except Exception as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        max_page = None
        if size:
            max_page = min(ceil(items_count / size), self.max_pages_count)

        result = {
            "current_page": page,
            "items": [self.add_chart_data(channel.to_dict(skip_empty=False)) for channel in channels],
            "items_count": items_count,
            "max_page": max_page,
            "aggregations": aggregations
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

    def get_fields(self, query_params, allowed_sections_to_load):
        fields = query_params.get("fields", [])

        if fields:
            fields = fields.split(",")

        fields = [
            field
            for field in fields
            if field.split(".")[0] in allowed_sections_to_load
        ]

        return fields

    @staticmethod
    def add_chart_data(channel):
        """ Generate and add chart data for channel """
        if not channel.get("stats"):
            channel["chart_data"] = {}
            channel["stats"] = {}
            return channel

        items = []
        items_count = 0
        history = zip(
            reversed(channel["stats"].get("subscribers_history")) if channel["stats"].get("subscribers_history") else [],
            reversed(channel["stats"].get("views_history")) if channel["stats"].get("views_history") else []
        )
        for subscribers, views in history:
            timestamp = channel["stats"].get("historydate") - timedelta(
                days=len(channel["stats"].get("subscribers_history")) - items_count - 1)
            timestamp = datetime.combine(timestamp, datetime.max.time())
            items_count += 1
            if any((subscribers, views)):
                items.append(
                    {"created_at": str(timestamp) + "Z",
                     "subscribers": subscribers,
                     "views": views}
                )
        channel["chart_data"] = items
        return channel

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


class ChannelQueryGenerator(QueryGenerator):
    es_manager = ChannelManager()
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER