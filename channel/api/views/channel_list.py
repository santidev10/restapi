import re
from copy import deepcopy
From math import ceil

from django.conf import settings

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

from es_components.managers.channel import ChannelManager
from es_components.constants import Sections

from channel.api.mixins import ChannelYoutubeSearchMixin
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.api_views_mixins import SegmentFilterMixin
from utils.api.cassandra_export_mixin import CassandraExportMixinApiView
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


SORT_KEY = {
    "thirty_days_subscribers": "stats.last_30day_subscribers",
    "thirty_days_views": "stats.last_30day_views",
    "subscribers": "stats.subscribers",
    "views_per_video": "stats.views_per_video",
}


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
    page_size = 50
    max_pages_count = 200

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

        # query params validation
        is_query_params_valid, error = self._validate_query_params()

        if not is_query_params_valid:
            return Response({"error": error}, HTTP_400_BAD_REQUEST)

        query_params = deepcopy(request.query_params)

        channel_manager = ChannelManager((Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS, Sections.ADS_STATS))
        filters = [channel_manager.forced_filters()]

        own_channels = int(query_params.get("own_channels", "0"))
        user_can_see_own_channels = request.user.has_perm("userprofile.settings_my_yt_channels")

        if own_channels and not user_can_see_own_channels:
            return Response(self.empty_response)

        if own_channels and user_can_see_own_channels:
            channels_ids = list(request.user.channels.values_list("channel_id", flat=True))
            filters.append(
                channel_manager.ids_query(channels_ids)
            )

            if not channels_ids:
                return Response(self.empty_response)

        filters += self.get_filters_from_query_params(query_params)
        sort = self.get_sort_rule(query_params)

        size, offset = self.get_limits(query_params)

        try:
            items_count = channel_manager.search(filters=filters, sort=sort, limit=None).count()
            channels = channel_manager.search(filters=filters, sort=sort, limit=size, offset=offset).execute().hits
            aggregations = None
        except Exception as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        max_page = None
        if self.page_size:
            max_page = min(ceil(items_count / self.page_size), self.max_pages_count)

        result = {
            "current_page": self.current_page,
            "items": channels.to_dict(),
            "items_count": self.items_count,
            "max_page": max_page,
            "aggregations": aggregations
        }

        return Response(result)

    def get_limits(self, query_params):
        size = int(query_params.pop("size", [self.page_size]).pop())
        page = int(query_params.pop("page", [1]).pop())
        offset = 0 if page <= 1 else page - 1 * size

        return size, offset

    @staticmethod
    def get_sort_rule(query_params):
        sort_params = query_params.pop("sort", None)

        if sort_params:
            key, direction = sort_params[0].split(":")
            field = SORT_KEY.get(key)

            if field:
                return [{field: {"order": direction}}]


    @staticmethod
    def get_filters_from_query_params(query_params):
        manager = ChannelManager()

        def get_filter_range(field, name_min=None, name_max=None):
            min = query_params.pop(name_min, [None])[0]
            max = query_params.pop(name_max, [None])[0]

            if min and max:
                return manager.filter_range(field, gte=min, lte=max)

        filters = []

        country = query_params.pop("country", [None])[0]
        if country:
            filters.append(manager.filter_term("general_data.country", country))

        # todo
        language = query_params.pop("language", [None])[0]
        if language:
            filters.append(manager.filter_term("general_data.language", language))

        # min_subscribers_yt, max_subscribers_yt
        filters.append(
            get_filter_range("stats.subscribers", "min_subscribers_yt", "max_subscribers_yt")
        )

        # min_thirty_days_subscribers, max_thirty_days_subscribers
        filters.append(
            get_filter_range("stats.last_30day_subscribers",
                             "min_thirty_days_subscribers", "max_thirty_days_subscribers")
        )

        # min_thirty_days_views, max_thirty_days_views
        filters.append(
            get_filter_range("stats.last_30day_views",
                             "min_thirty_days_views", "max_thirty_days_views")
        )

        # min_sentiment, max_sentiment
        filters.append(
            get_filter_range("stats.sentiment", "min_sentiment", "max_sentiment")
        )

        # min_engage_rate, max_engage_rate
        filters.append(
            get_filter_range("stats.engage_rate", "min_engage_rate", "max_engage_rate")
        )

        # min_views_per_video, max_views_per_video
        filters.append(
            get_filter_range("stats.views_per_video", "min_views_per_video", "max_views_per_video")
        )

        # min_subscribers_fb, max_subscribers_fb
        filters.append(
            get_filter_range("social.facebook_likes", "min_subscribers_fb", "max_subscribers_fb")
        )

        # min_subscribers_tw, max_subscribers_tw
        filters.append(
            get_filter_range("social.twitter_followers", "min_subscribers_tw", "max_subscribers_tw")
        )

        # min_subscribers_in, max_subscribers_in
        filters.append(
            get_filter_range("social.instagram_followers", "min_subscribers_in", "max_subscribers_in")
        )

        # category
        category = query_params.pop("category", [None])[0]
        if category is not None:
            # regexp = "|".join([".*" + c + ".*" for c in category.split(",")])
            # query_params.update(category__regexp=regexp)

            filters.append(
                manager.filter_term("general_data.top_category", category)
            )

        # todo text_search
        text_search = query_params.pop("text_search", [None])[0]
        if text_search:
            query_params.update(text_search__match_phrase=text_search)

        # todo channel_group
        # make("term", "channel_group")
        channel_group = query_params.pop("channel_group", [None])[0]
        if channel_group:
            filters.append(manager.filter_term("channel_group", channel_group))

        return [filter for filter in filters if filter is not None]

    @staticmethod
    def adapt_response_data(channels, user):
        """
        Adapt SDB response format
        """
        # user_channels = set(user.channels.values_list("channel_id", flat=True))
        # for channel in channels:
        #     channel.main.is_owner = channel.main.id in user_channels
        #
        #     # if "country" in item and item["country"] is None:
        #     #     item["country"] = ""
        #     # if "history_date" in item and item["history_date"]:
        #     #     item["history_date"] = item["history_date"][:10]
        #
        #     if channel.stats.historydate:
        #         channel.stats.historydate = channel.stats.historydate[:10]
        #
        #     if not (user.has_perm('userprofile.channel_audience') or channel.main.is_owner):
        #
        #         item['has_audience'] = False
        #         item["verified"] = False
        #         item.pop('audience', None)
        #         item['brand_safety'] = None
        #         item['safety_chart_data'] = None
        #         item.pop('traffic_sources', None)
        #
        #     if not user.is_staff:
        #         item.pop("cms__title", None)
        #
        #     for field in ["youtube_published_at", "updated_at"]:
        #         if field in item and item[field]:
        #             item[field] = re.sub(
        #                 "^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+|)$",
        #                 "\g<0>Z",
        #                 item[field]
        #             )
        # return response_data

    def _data_filtered_batch_generator(self, filters):
        return Connector().get_channel_list_full(filters, fields=ChannelListCSVRendered.header, batch_size=1000)
