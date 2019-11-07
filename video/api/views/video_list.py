"""
Video api views module
"""
from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from audit_tool.models import BlacklistItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from video.api.serializers.video import VideoSerializer
from video.api.serializers.video_with_blacklist_data import VideoWithBlackListSerializer
from video.constants import TERMS_FILTER
from video.constants import MATCH_PHRASE_FILTER
from video.constants import RANGE_FILTER
from video.constants import EXISTS_FILTER
from video.constants import HISTORY_FIELDS
from utils.permissions import BrandSafetyDataVisible

from cache.models import CacheItem


class VideoListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.video_list"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            IsAdminUser
        ),
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
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter,)

    allowed_aggregations = (
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "analytics:exists",
        "analytics:missing",
        "cms.cms_title",
        "general_data.category",
        "general_data.country",
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
        "brand_safety",
        "stats.flags",
        "custom_captions.items:exists",
        "custom_captions.items:missing",
        "captions:exists",
        "captions:missing",
    )

    allowed_percentiles = (
        "ads_stats.average_cpv:percentiles",
        "ads_stats.ctr_v:percentiles",
        "ads_stats.video_view_rate:percentiles",
        "stats.channel_subscribers:percentiles",
        "stats.last_day_views:percentiles",
        "stats.views:percentiles",
    )

    aggregations_key = "video_aggregations"
    cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=aggregations_key)
    cached_aggregations = cached_aggregations_object.value

    blacklist_data_type = BlacklistItem.VIDEO_ITEM

    def get_serializer_class(self):
        if self.request and self.request.user and (
                self.request.user.is_staff or self.request.user.has_perm("userprofile.flag_audit")):
            return VideoWithBlackListSerializer
        return VideoSerializer

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                    Sections.CUSTOM_CAPTIONS)

        channel_id = deepcopy(self.request.query_params).get("channel")
        flags = deepcopy(self.request.query_params).get("flags")

        if channel_id:
            self.request.query_params._mutable = True
            self.request.query_params["channel.id"] = [channel_id]
            self.request.query_params._mutable = False

        if flags:
            self.request.query_params._mutable = True
            flags = flags.lower().replace(" ", "_")
            self.request.query_params["stats.flags"] = flags
            self.terms_filter += ("stats.flags",)
            self.request.query_params._mutable = False

        if not self.request.user.has_perm("userprofile.transcripts_filter") and \
                not self.request.user.is_staff:
            if "transcripts" in self.request.query_params:
                self.request.query_params._mutable = True
                self.request.query_params["transcripts"] = None
                self.request.query_params._mutable = False

        if not BrandSafetyDataVisible().has_permission(self.request):
            if "brand_safety" in self.request.query_params:
                self.request.query_params._mutable = True
                self.request.query_params["brand_safety"] = None
                self.request.query_params._mutable = False

        if not self.request.user.has_perm("userprofile.video_list") and \
                not self.request.user.has_perm("userprofile.view_highlights"):
            user_channels_ids = set(self.request.user.channels.values_list("channel_id", flat=True))

            if channel_id and (channel_id in user_channels_ids):
                sections += (Sections.ANALYTICS,)

        if self.request.user.is_staff or \
                self.request.user.has_perm("userprofile.video_audience"):
            sections += (Sections.ANALYTICS,)
        return ESQuerysetAdapter(VideoManager(sections))
