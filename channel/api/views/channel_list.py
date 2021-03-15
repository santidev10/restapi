from copy import deepcopy

from rest_framework.generics import ListAPIView

from audit_tool.models import IASHistory
from cache.constants import ADMIN_CHANNEL_AGGREGATIONS_KEY
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from cache.models import CacheItem
from channel.api.serializers.channel import ChannelSerializer
from channel.constants import EXISTS_FILTER
from channel.constants import MATCH_PHRASE_FILTER
from channel.constants import RANGE_FILTER
from channel.constants import TERMS_FILTER
from channel.models import AuthChannel
from channel.utils import ChannelGroupParamAdapter
from channel.utils import IsTrackedParamsAdapter
from channel.utils import VettedParamsAdapter
from es_components.constants import Sections
from es_components.countries import COUNTRIES
from es_components.languages import LANGUAGES
from es_components.managers.channel import ChannelManager
from es_components.managers.channel import VettingAdminChannelManager
from userprofile.constants import StaticPermissions
from utils.aggregation_constants import ALLOWED_CHANNEL_AGGREGATIONS
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.mutate_query_params import AddFieldsMixin
from utils.api.mutate_query_params import BrandSuitabilityFiltersMixin
from utils.api.mutate_query_params import ValidYoutubeIdMixin
from utils.api.mutate_query_params import VettingAdminAggregationsMixin
from utils.api.mutate_query_params import mutate_query_params
from utils.api.research import ESEmptyResponseAdapter
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import AggregationFiltersPermission
from utils.permissions import BrandSafetyDataVisible


class ChannelsNotFound(Exception):
    pass


class UserChannelsNotAvailable(Exception):
    pass


class ChannelESFilterBackend(ESFilterBackend):

    def _get_query_generator_context(self) -> dict:
        """
        provide a context dictionary for the query generator on instantiation
        :return:
        """
        return {
            "ias_last_ingested_timestamp": IASHistory.get_last_ingested_timestamp(),
            "auth_channel_ids": AuthChannel.get_auth_channel_ids(),
        }

    @staticmethod
    def __get_similar_channels(query_params):
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
        return None

    def filter_queryset(self, request, queryset, view):
        try:
            similar_channels_ids = self.__get_similar_channels(deepcopy(request.query_params))

            if similar_channels_ids:
                with mutate_query_params(request.query_params):
                    request.query_params["main.id"] = list(similar_channels_ids)
        except ChannelsNotFound:
            queryset = ESEmptyResponseAdapter(ChannelManager())

        result = super(ChannelESFilterBackend, self).filter_queryset(request, queryset, view)
        return result


class ChannelListApiView(BrandSuitabilityFiltersMixin, VettingAdminAggregationsMixin, AddFieldsMixin, ValidYoutubeIdMixin,
                         APIViewMixin, ListAPIView):
    permission_classes = (
        AggregationFiltersPermission,
    )
    filter_backends = (FreeFieldOrderingFilter, ChannelESFilterBackend)
    pagination_class = ResearchPaginator
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.last_7day_views:desc",
        "stats.last_day_views:desc",
        "stats.views:desc",
        "stats.last_30day_subscribers:desc",
        "stats.last_7day_subscribers:desc",
        "stats.last_day_subscribers:desc",
        "stats.subscribers:desc",
        "stats.sentiment:desc",
        "stats.views_per_video:desc",
        "stats.last_30day_views:asc",
        "stats.last_7day_views:asc",
        "stats.last_day_views:asc",
        "stats.views:asc",
        "stats.last_30day_subscribers:asc",
        "stats.last_7day_subscribers:asc",
        "stats.last_day_subscribers:asc",
        "stats.subscribers:asc",
        "stats.sentiment:asc",
        "stats.views_per_video:asc",
        "general_data.youtube_published_at:desc",
        "general_data.youtube_published_at:asc",
        "brand_safety.overall_score:desc",
        "brand_safety.overall_score:asc",
        "_score:desc",
        "_score:asc"
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter, ChannelGroupParamAdapter, VettedParamsAdapter, IsTrackedParamsAdapter)
    allowed_aggregations = ALLOWED_CHANNEL_AGGREGATIONS
    cached_aggregations_key = CHANNEL_AGGREGATIONS_KEY
    admin_cached_aggregations_key = ADMIN_CHANNEL_AGGREGATIONS_KEY
    manager_class = ChannelManager
    admin_manager_class = VettingAdminChannelManager
    serializer_class = ChannelSerializer

    cache_class = CacheItem

    allowed_percentiles = (
        "ads_stats.average_cpv:percentiles",
        "ads_stats.average_cpm:percentiles",
        "ads_stats.video_view_rate:percentiles",
        "ads_stats.ctr:percentiles",
        "ads_stats.ctr_v:percentiles",
        "ads_stats.video_quartile_100_rate:percentiles",
        "stats.last_30day_subscribers:percentiles",
        "stats.last_30day_views:percentiles",
        "stats.subscribers:percentiles",
        "stats.views_per_video:percentiles",
    )

    def get_serializer_context(self):
        context = {
            "user": self.request.user,
            "latest_ias_ingestion": IASHistory.get_last_ingested_timestamp(),
            "countries_map": {code.upper(): name[0] for code, name in COUNTRIES.items()},
            "languages_map": {code.lower(): name for code, name in LANGUAGES.items()},
        }
        return context

    @staticmethod
    def _get_manager_context():
        """
        return a context dictionary for the ChannelManager instance
        :return:
        """
        return {
            "ias_last_ingested_timestamp": IASHistory.get_last_ingested_timestamp(),
            "auth_channel_ids": AuthChannel.get_auth_channel_ids(),
        }

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                    Sections.TASK_US_DATA, Sections.IAS_DATA)
        try:
            channels_ids = self.get_own_channel_ids(self.request.user, deepcopy(self.request.query_params))
        except UserChannelsNotAvailable:
            return ESEmptyResponseAdapter(ChannelManager())

        if channels_ids:
            with mutate_query_params(self.request.query_params):
                self.request.query_params["main.id"] = channels_ids

        if not BrandSafetyDataVisible().has_permission(self.request):
            if "brand_safety" in self.request.query_params:
                with mutate_query_params(self.request.query_params):
                    self.request.query_params["brand_safety"] = None

        if self.request.user.has_permission(StaticPermissions.RESEARCH__MONETIZATION):
            sections += (Sections.MONETIZATION,)
        else:
            with mutate_query_params(self.request.query_params):
                try:
                    del self.request.query_params["monetization.is_monetizable"]
                except KeyError:
                    pass

        self.guard_vetting_data_perm_aggregations()

        self.guard_brand_suitability_high_risk_filters()

        self.ensure_exact_youtube_id_result(manager=ChannelManager())

        self.add_fields()
        is_default_page = self.is_default_page()
        return ESQuerysetAdapter(self.get_manager_class()(sections, context=self._get_manager_context()),
                                 cached_aggregations=self.get_cached_aggregations(),
                                 is_default_page=is_default_page)

    @staticmethod
    def get_own_channel_ids(user, query_params):
        own_channels = int(query_params.get("own_channels", "0"))
        user_can_see_own_channels = user.has_permission(StaticPermissions.RESEARCH)

        if own_channels and not user_can_see_own_channels:
            raise UserChannelsNotAvailable

        if own_channels and user_can_see_own_channels:
            channels_ids = list(user.channels.values_list("channel_id", flat=True))

            if not channels_ids:
                raise UserChannelsNotAvailable

            return channels_ids
        return None
