from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from channel.api.serializers.channel import ChannelSerializer
from channel.api.serializers.channel_with_blacklist_data import ChannelWithBlackListSerializer
from channel.constants import TERMS_FILTER
from channel.constants import MATCH_PHRASE_FILTER
from channel.constants import RANGE_FILTER
from channel.constants import EXISTS_FILTER
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.research import ESEmptyResponseAdapter
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class ChannelsNotFound(Exception):
    pass


class UserChannelsNotAvailable(Exception):
    pass


class ChannelESFilterBackend(ESFilterBackend):
    def __get_similar_channels(self, query_params):
        similar_to = query_params.get("similar_to", None)
        if similar_to:
            channel = ChannelManager(Sections.SIMILAR_CHANNELS).get([similar_to]).pop()

            if not (channel and channel.similar_channels):
                raise ChannelsNotFound

            cluster = query_params.get("similar_cluster", "default")
            similar_channels_ids = getattr(channel.similar_channels, cluster)

            if not similar_channels_ids:
                raise ChannelsNotFound

            return similar_channels_ids

    def filter_queryset(self, request, queryset, view):
        try:
            similar_channels_ids = self.__get_similar_channels(deepcopy(request.query_params))

            if similar_channels_ids:
                request.query_params._mutable = True
                request.query_params["main.id"] = list(similar_channels_ids)
        except ChannelsNotFound:
            queryset = ESEmptyResponseAdapter(ChannelManager())

        result = super(ChannelESFilterBackend, self).filter_queryset(request, queryset, view)
        return result


class ChannelListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.channel_list"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            IsAdminUser
        ),
    )
    filter_backends = (FreeFieldOrderingFilter, ChannelESFilterBackend)
    pagination_class = ResearchPaginator
    ordering_fields = (
        "stats.last_30day_subscribers:desc",
        "stats.last_30day_views:desc",
        "stats.subscribers:desc",
        "stats.sentiment:desc",
        "stats.views_per_video:desc",
        "stats.last_30day_subscribers:asc",
        "stats.last_30day_views:asc",
        "stats.subscribers:asc",
        "stats.sentiment:asc",
        "stats.views_per_video:asc",
        "general_data.youtube_published_at:desc",
        "general_data.youtube_published_at:asc"
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
        "ads_stats:exists",
        "analytics.age13_17:max",
        "analytics.age13_17:min",
        "analytics.age18_24:max",
        "analytics.age18_24:min",
        "analytics.age25_34:max",
        "analytics.age25_34:min",
        "analytics.age35_44:max",
        "analytics.age35_44:min",
        "analytics.age45_54:max",
        "analytics.age45_54:min",
        "analytics.age55_64:max",
        "analytics.age55_64:min",
        "analytics.age65_:max",
        "analytics.age65_:min",
        "cms.cms_title",
        "analytics.gender_female:max",
        "analytics.gender_female:min",
        "analytics.gender_male:max",
        "analytics.gender_male:min",
        "analytics.gender_other:max",
        "analytics.gender_other:min",
        "analytics:exists",
        "analytics:missing",
        "general_data.emails:exists",
        "general_data.emails:missing",
        "custom_properties.preferred",
        "general_data.country",
        "general_data.top_category",
        "general_data.top_language",
        "social.facebook_likes:max",
        "social.facebook_likes:min",
        "social.instagram_followers:max",
        "social.instagram_followers:min",
        "social.twitter_followers:max",
        "social.twitter_followers:min",
        "stats.last_30day_subscribers:max",
        "stats.last_30day_subscribers:min",
        "stats.last_30day_views:max",
        "stats.last_30day_views:min",
        "stats.subscribers:max",
        "stats.subscribers:min",
        "stats.views_per_video:max",
        "stats.views_per_video:min",
        "brand_safety",
        "stats.channel_group"
    )

    allowed_percentiles = (
        "ads_stats.average_cpv:percentiles",
        "ads_stats.video_view_rate:percentiles",
        "ads_stats.ctr_v:percentiles",
        "social.facebook_likes:percentiles",
        "social.instagram_followers:percentiles",
        "social.twitter_followers:percentiles",
        "stats.last_30day_subscribers:percentiles",
        "stats.last_30day_views:percentiles",
        "stats.subscribers:percentiles",
        "stats.views_per_video:percentiles",
    )

    def get_serializer_class(self):
        if self.request and self.request.user and (
                self.request.user.is_staff or self.request.user.has_perm("userprofile.flag_audit")):
            return ChannelWithBlackListSerializer
        return ChannelSerializer

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS)
        try:
            channels_ids = self.get_own_channel_ids(self.request.user, deepcopy(self.request.query_params))
        except UserChannelsNotAvailable:
            return ESEmptyResponseAdapter(ChannelManager())

        if channels_ids:
            self.request.query_params._mutable = True
            self.request.query_params["main.id"] = channels_ids
            self.request.query_params._mutable = False

        if self.request.user.is_staff or self.request.user.has_perm("userprofile.scoring_brand_safety"):
            if "brand_safety" in self.request.query_params:
                self.request.query_params._mutable = True
                label = self.request.query_params["brand_safety"].lower()
                if label == "safe":
                    self.request.query_params["brand_safety.overall_score"] = "90,100"
                elif label == "low risk":
                    self.request.query_params["brand_safety.overall_score"] = "80,89"
                elif label == "risky":
                    self.request.query_params["brand_safety.overall_score"] = "70,79"
                elif label == "high risk":
                    self.request.query_params["brand_safety.overall_score"] = "0,69"
                self.request.query_params._mutable = False

        if self.request.user.is_staff or channels_ids or self.request.user.has_perm("userprofile.channel_audience"):
            sections += (Sections.ANALYTICS,)

        result = ESQuerysetAdapter(ChannelManager(sections))

        return result

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
