"""
Video api views module
"""
import re
from copy import deepcopy
from datetime import timedelta
from datetime import datetime

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.managers.video import VideoManager
from es_components.constants import Sections

from utils.api.research import ResearchPaginator
from utils.api.research import ESBrandSafetyFilterBackend
from utils.api.research import ESQuerysetResearchAdapter
from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import APIViewMixin
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


TERMS_FILTER = ("general_data.country", "general_data.language", "general_data.category",
                "analytics.verified", "analytics.cms_title", "channel.id", "channel.title",
                "monetization.is_monetizable", "monetization.channel_preferred",
                "channel.id", "general_data.tags",)

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("stats.views", "stats.engage_rate", "stats.sentiment", "stats.views_per_day",
                "stats.channel_subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_", "general.youtube_published_at")

EXISTS_FILTER = ("ads_stats", "analytics", "stats.flags")


REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"
HISTORY_FIELDS = ("stats.views_history", "stats.likes_history", "stats.dislikes_history",
                  "stats.comments_history", "stats.historydate",)


def add_chart_data(videos):
    """ Generate and add chart data for channel """
    for video in videos:
        if not video.stats:
            video.chart_data = []
            continue

        chart_data = []
        items_count = 0
        history = zip(
            reversed(video.stats.views_history or []),
            reversed(video.stats.likes_history or []),
            reversed(video.stats.dislikes_history or []),
            reversed(video.stats.comments_history or [])
        )
        for views, likes, dislikes, comments in history:
            timestamp = video.stats.historydate - timedelta(
                days=len(video.stats.views_history) - items_count - 1)
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
        video.chart_data = chart_data
    return videos


def add_transcript(videos):
    for video in videos:
        transcript = None
        if video.captions and video.captions.items:
            for caption in video.captions.items:
                if caption.language_code == "en":
                    text = caption.text
                    transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        video.transcript = transcript
    return videos


class VideoListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.video_list"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            IsAdminUser
        ),
    )

    filter_backends = (FreeFieldOrderingFilter, ESBrandSafetyFilterBackend)
    pagination_class = ResearchPaginator

    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.views:desc",
        "stats.likes:desc",
        "stats.dislikes:desc",
        "stats.comments:desc",
        "stats.sentiment:desc",
        "general_data.youtube_published_at:desc",
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER

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
        "stats.flags:exists",
        "stats.flags:missing",
        "stats.channel_subscribers:max",
        "stats.channel_subscribers:min",
        "stats.last_day_views:max",
        "stats.last_day_views:min",
        "stats.views:max",
        "stats.views:min",
    )

    allowed_percentiles = (
        "ads_stats.average_cpv:percentiles",
        "ads_stats.ctr_v:percentiles",
        "ads_stats.video_view_rate:percentiles",
        "stats.channel_subscribers:percentiles",
        "stats.last_day_views:percentiles",
        "stats.views:percentiles",
    )

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS,)

        channel_id = deepcopy(self.request.query_params).get("channel")
        flags = deepcopy(self.request.query_params).get("flags")

        if channel_id:
            self.request.query_params._mutable = True
            self.request.query_params["channel.id"] = [channel_id]

        if flags:
            self.request.query_params._mutable = True
            self.request.query_params["stats.flags"] = flags
            self.terms_filter += ("stats.flags",)

        if not self.request.user.has_perm("userprofile.video_list") and \
                not self.request.user.has_perm("userprofile.view_highlights"):
            user_channels_ids = set(self.request.user.channels.values_list("channel_id", flat=True))

            if channel_id and (channel_id in user_channels_ids):
                sections += (Sections.ANALYTICS,)

        if self.request.user.is_staff or \
                self.request.user.has_perm("userprofile.video_audience"):
            sections += (Sections.ANALYTICS,)
        return ESQuerysetResearchAdapter(VideoManager(sections), max_items=100)\
            .extra_fields_func((add_chart_data, add_transcript,))