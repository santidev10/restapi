"""
Video api views module
"""
import re
from copy import deepcopy
from datetime import timedelta
from datetime import datetime
from math import ceil

from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView
from rest_framework_csv.renderers import CSVStreamingRenderer

from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_VIDEO_DETAILS_FIELDS
from singledb.settings import DEFAULT_VIDEO_LIST_FIELDS

from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.connections import init_es_connection
from utils.api.cassandra_export_mixin import CassandraExportMixinApiView
from utils.api_views_mixins import SegmentFilterMixin
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.es_components_api_utils import get_limits
from utils.es_components_api_utils import get_sort_rule
from utils.es_components_api_utils import QueryGenerator
from utils.es_components_api_utils import get_fields
from utils.celery.dmp_celery import send_task_delete_videos


init_es_connection()

TERMS_FILTER = ("general_data.country", "general_data.language", "general_data.category",
                "analytics.verified", "analytics.cms_title", "channel.id", "channel.title",
                "monetization.is_monetizable", "monetization.channel_preferred", "stats.flags")

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("stats.views", "stats.engage_rate", "stats.sentiment", "stats.views_per_day",
                "stats.channel_subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_", "general.youtube_published_at")

EXISTS_FILTER = ("ads_stats", "analytics")



class VideoListCSVRendered(CSVStreamingRenderer):
    header = [
        "title",
        "url",
        "views",
        "likes",
        "dislikes",
        "comments",
        "youtube_published_at",
        "brand_safety_score",
        "video_view_rate",
        "ctr",
        "ctr_v",
        "average_cpv"
    ]


REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"
HISTORY_FIELDS = ("stats.views_history", "stats.likes_history", "stats.dislikes_history",
                  "stats.comments_history", "stats.historydate",)


def add_transcript(video):
    transcript = None
    if video.get("captions") and video["captions"].get("items"):
        for caption in video["captions"].get("items"):
            text = caption.get("text")
            if caption.get("language_code") == "en" and text:
                transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
    video["transcript"] = transcript
    return video


def add_chart_data(video):
    if not video.get("stats"):
        video["chart_data"] = []
        return video

    chart_data = []
    items_count = 0
    history = zip(
        reversed(video["stats"].get("views_history") or []),
        reversed(video["stats"].get("likes_history") or []),
        reversed(video["stats"].get("dislikes_history") or []),
        reversed(video["stats"].get("comments_history") or [])
    )
    for views, likes, dislikes, comments in history:
        timestamp = video["stats"].get("historydate") - timedelta(
                days=len(video["stats"].get("views_history")) - items_count - 1)
        timestamp = datetime.combine(timestamp, datetime.max.time())
        items_count += 1
        if any((views, likes, dislikes, comments)):
            chart_data.append(
                {"created_at": "{}{}".format(str(timestamp), "Z"),
                 "views": views,
                 "likes": likes,
                 "dislikes": dislikes,
                 "comments": comments}
            )
    video["chart_data"] = chart_data
    return video


def add_extra_field(video):
    video = add_chart_data(video)
    video = add_transcript(video)
    return video


class VideoListApiView(APIView, CassandraExportMixinApiView, PermissionRequiredMixin, SegmentFilterMixin):
    """
    Proxy view for video list
    """
    # TODO Check additional auth logic
    permission_classes = tuple()
    permission_required = (
        "userprofile.video_list",
        "userprofile.settings_my_yt_channels"
    )
    renderer = VideoListCSVRendered
    export_file_title = "video"
    default_request_fields = DEFAULT_VIDEO_LIST_FIELDS
    empty_response = {
        "max_page": 1,
        "items_count": 0,
        "items": [],
        "current_page": 1,
    }
    es_manager = VideoManager
    max_pages_count = 200
    allowed_aggregations = (
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "analytics.cms_title",
        "analytics:exists",
        "analytics:missing",
        "general_data.category",
        "general_data.language",
        "general_data.youtube_published_at:max",
        "general_data.youtube_published_at:min",
        "is_flagged:count",
        "stats.channel_subscribers:max",
        "stats.channel_subscribers:min",
        "stats.last_day_views:max",
        "stats.last_day_views:min",
        "stats.views:max",
        "stats.views:min",
        # FIXME: Disabled because of overloading of ES by these aggregations
        # "ads_stats.average_cpv:percentiles",
        # "ads_stats.ctr_v:percentiles",
        # "ads_stats.video_view_rate:percentiles",
        # "stats.channel_subscribers:percentiles",
        # "stats.last_day_views:percentiles",
        # "stats.views:percentiles",
    )

    @add_brand_safety_data
    def get(self, request):
        is_query_params_valid, error = self._validate_query_params()
        if not is_query_params_valid:
            return Response({"error": error}, HTTP_400_BAD_REQUEST)

        query_params = deepcopy(request.query_params)

        allowed_sections_to_load = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION,
                                    Sections.CAPTIONS,)

        channel_id = query_params.get("channel")

        if not request.user.has_perm("userprofile.video_list") and \
                not request.user.has_perm("userprofile.view_highlights"):
            user_channels_ids = set(request.user.channels.values_list("channel_id", flat=True))

            if channel_id and (channel_id in user_channels_ids):
                allowed_sections_to_load += (Sections.ANALYTICS,)

        if request.user.is_staff or \
                request.user.has_perm("userprofile.video_audience"):
                allowed_sections_to_load += (Sections.ANALYTICS,)

        es_manager = self.es_manager(allowed_sections_to_load)

        filters = VideoQueryGenerator(query_params).get_search_filters(channel_id)
        sort = get_sort_rule(query_params)
        size, offset, page = get_limits(query_params)

        fields_to_load = get_fields(query_params, allowed_sections_to_load) + list(HISTORY_FIELDS)

        try:
            items_count = es_manager.search(filters=filters, sort=sort, limit=None).count()
            videos = es_manager.search(filters=filters, sort=sort, limit=size + offset, offset=offset) \
                .source(includes=fields_to_load).execute().hits

            aggregations = self._get_aggregations(es_manager, filters, query_params)

        except Exception as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        max_page = None
        if size:
            max_page = min(ceil(items_count / size), self.max_pages_count)

        result = {
            "current_page": page,
            "items": [add_extra_field(video.to_dict()) for video in videos],
            "items_count": items_count,
            "max_page": max_page,
            "aggregations": aggregations
        }
        return Response(result)


    def _get_aggregations(self, es_manager, filters, query_params):
        aggregation_properties_str = query_params.get("aggregations", "")
        aggregation_properties = [
            prop
            for prop in aggregation_properties_str.split(",")
            if prop in self.allowed_aggregations
        ]
        aggregations = es_manager.get_aggregation(
            es_manager.search(filters=filters, limit=None),
            properties=aggregation_properties
        )
        return aggregations

    def _data_filtered_batch_generator(self, filters):
        return Connector().get_video_list_full(filters, fields=VideoListCSVRendered.header, batch_size=1000)


class VideoRetrieveUpdateApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    permission_required = ("userprofile.video_details",)
    default_request_fields = DEFAULT_VIDEO_DETAILS_FIELDS

    __video_manager = VideoManager

    def video_manager(self, sections=None):
        if sections or self.__video_manager is None:
            self.__video_manager = VideoManager(sections)
        return self.__video_manager

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        video_id = kwargs.get('pk')

        allowed_sections_to_load = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION,
                                    Sections.CAPTIONS,)

        fields_to_load = get_fields(request.query_params, allowed_sections_to_load) + list(HISTORY_FIELDS)

        video = self.video_manager(allowed_sections_to_load).model.get(video_id, _source=fields_to_load)

        if not video:
            return Response(data={"error": "Channel not found"}, status=HTTP_404_NOT_FOUND)

        user_channels = set(self.request.user.channels.values_list("channel_id", flat=True))

        result = add_extra_field(video.to_dict())

        if not(video.channel.id in user_channels or self.request.user.has_perm("userprofile.video_audience") \
                or not self.request.user.is_staff):
            result[Sections.ANALYTICS] = {}

        return Response(result)


class VideoSetApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)

    def delete(self, request, *args, **kwargs):
        video_ids = request.data.get("delete", [])
        send_task_delete_videos((video_ids,))
        return Response()


class VideoQueryGenerator(QueryGenerator):
    es_manager = VideoManager()
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER

    def get_search_filters(self, channel_id=None):
        filters = super(VideoQueryGenerator, self).get_search_filters()

        if channel_id:
            filters.append(self.es_manager.by_channel_ids_query(channel_id))

        return filters
