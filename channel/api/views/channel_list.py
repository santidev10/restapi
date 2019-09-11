from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from channel.api.serializers.channel import ChannelSerializer
from channel.api.serializers.channel_with_blacklist_data import ChannelWithBlackListSerializer
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

TERMS_FILTER = ("general_data.country", "general_data.top_language", "general_data.top_category",
                "custom_properties.preferred", "analytics.verified", "cms.cms_title",
                "stats.channel_group", "main.id")

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("social.instagram_followers", "social.twitter_followers", "social.facebook_likes",
                "stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_")

EXISTS_FILTER = ("general_data.emails", "ads_stats", "analytics")


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
        "stats.last_7day_views:desc",
        "stats.last_day_views:desc",
        "stats.views:desc",
        "stats.subscribers:desc",
        "stats.sentiment:desc",
        "stats.views_per_video:desc",
        "stats.last_30day_subscribers:asc",
        "stats.last_30day_views:asc",
        "stats.last_7day_views:asc",
        "stats.last_day_views:asc",
        "stats.views:asc",
        "stats.subscribers:asc",
        "stats.sentiment:asc",
        "stats.views_per_video:asc",
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
