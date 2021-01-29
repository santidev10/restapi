"""
Video api views module
"""
from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from audit_tool.models import BlacklistItem
from cache.constants import ADMIN_VIDEO_AGGREGATIONS_KEY
from cache.constants import VIDEO_AGGREGATIONS_KEY
from cache.models import CacheItem
from channel.utils import VettedParamsAdapter
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers.video import VettingAdminVideoManager
from es_components.managers.video import VideoManager
from userprofile.constants import StaticPermissions
from utils.aggregation_constants import ALLOWED_VIDEO_AGGREGATIONS
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.mutate_query_params import AddFieldsMixin
from utils.api.mutate_query_params import ValidYoutubeIdMixin
from utils.api.mutate_query_params import VettingAdminAggregationsMixin
from utils.api.mutate_query_params import VettingDataPermFiltersMixin
from utils.api.mutate_query_params import mutate_query_params
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import FlagsParamAdapter
from utils.es_components_api_utils import SentimentParamAdapter
from utils.permissions import BrandSafetyDataVisible
from video.api.serializers.video import VideoAdminSerializer
from video.api.serializers.video import VideoSerializer
from video.api.serializers.video import VideoWithVettedStatusSerializer
from video.constants import EXISTS_FILTER
from video.constants import MATCH_PHRASE_FILTER
from video.constants import RANGE_FILTER
from video.constants import TERMS_FILTER


class VideoListApiView(VettingDataPermFiltersMixin, VettingAdminAggregationsMixin, AddFieldsMixin, ValidYoutubeIdMixin,
                       APIViewMixin, ListAPIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.RESEARCH),
    )

    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)
    pagination_class = ResearchPaginator

    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.last_7day_views:desc",
        "stats.last_day_views:desc",
        "stats.views:desc",
        "stats.likes:desc",
        "stats.dislikes:desc",
        "stats.last_7day_comments:desc",
        "stats.last_day_comments:desc",
        "stats.comments:desc",
        "stats.sentiment:desc",
        "general_data.youtube_published_at:desc",
        "stats.last_30day_views:asc",
        "stats.last_7day_views:asc",
        "stats.last_day_views:asc",
        "stats.views:asc",
        "stats.likes:asc",
        "stats.dislikes:asc",
        "stats.last_7day_comments:asc",
        "stats.last_day_comments:asc",
        "stats.comments:asc",
        "stats.sentiment:asc",
        "general_data.youtube_published_at:asc",
        "brand_safety.overall_score:desc",
        "brand_safety.overall_score:asc",
        "_score:desc",
        "_score:asc",
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter, VettedParamsAdapter, SentimentParamAdapter, FlagsParamAdapter)
    allowed_aggregations = ALLOWED_VIDEO_AGGREGATIONS
    cached_aggregations_key = VIDEO_AGGREGATIONS_KEY
    admin_cached_aggregations_key = ADMIN_VIDEO_AGGREGATIONS_KEY
    manager_class = VideoManager
    admin_manager_class = VettingAdminVideoManager

    cache_class = CacheItem

    allowed_percentiles = (
        "ads_stats.average_cpv:percentiles",
        "ads_stats.average_cpm:percentiles",
        "ads_stats.ctr:percentiles",
        "ads_stats.ctr_v:percentiles",
        "ads_stats.video_view_rate:percentiles",
        "ads_stats.video_quartile_100_rate:percentiles",
        "stats.channel_subscribers:percentiles",
        "stats.last_day_views:percentiles",
        "stats.views:percentiles",
        "stats.sentiment:percentiles",
    )

    blacklist_data_type = BlacklistItem.VIDEO_ITEM

    def get_serializer_class(self):
        if self.request and self.request.user and self.request.user.has_permission(StaticPermissions.ADMIN):
            return VideoAdminSerializer
        if self.request.user.has_permission(StaticPermissions.RESEARCH__VETTING_DATA):
            return VideoWithVettedStatusSerializer
        return VideoSerializer

    def get_serializer_context(self):
        channel_manager = ChannelManager([Sections.CUSTOM_PROPERTIES])
        channel_ids = [video.channel.id for video in self.paginator.page.object_list if video.channel.id is not None]
        context = {
            "channel_blocklist": {
                channel.main.id: channel.custom_properties.blocklist
                for channel in channel_manager.get(channel_ids, skip_none=True)
            }
        }
        return context

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                    Sections.CUSTOM_CAPTIONS, Sections.TASK_US_DATA, Sections.CUSTOM_PROPERTIES,)

        channel_id = deepcopy(self.request.query_params).get("channel")

        if channel_id:
            with mutate_query_params(self.request.query_params):
                self.request.query_params["channel.id"] = [channel_id]

        if not self.request.user.has_permission(StaticPermissions.RESEARCH__TRANSCRIPTS) and \
            not self.request.user.has_permission(StaticPermissions.ADMIN):
            if "transcripts" in self.request.query_params:
                with mutate_query_params(self.request.query_params):
                    self.request.query_params["transcripts"] = None

        if not self.request.user.has_permission(StaticPermissions.RESEARCH__VETTING_DATA) \
                and not self.request.user.has_permission(StaticPermissions.ADMIN):
            vetted_params = ["task_us_data.age_group", "task_us_data.content_type", "task_us_data.gender"]
            with mutate_query_params(self.request.query_params):
                for param in vetted_params:
                    if param in self.request.query_params:
                        self.request.query_params[param] = None

        if not BrandSafetyDataVisible().has_permission(self.request):
            if "brand_safety" in self.request.query_params:
                with mutate_query_params(self.request.query_params):
                    self.request.query_params["brand_safety"] = None

        self.guard_vetting_data_perm_aggregations()

        self.guard_vetting_data_perm_filters()

        self.ensure_exact_youtube_id_result(manager=VideoManager())

        self.add_fields()
        is_default_page = self.is_default_page()
        return ESQuerysetAdapter(self.get_manager_class()(sections),
                                 cached_aggregations=self.get_cached_aggregations(),
                                 is_default_page=is_default_page)
