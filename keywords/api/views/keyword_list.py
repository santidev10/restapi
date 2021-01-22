from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from cache.constants import KEYWORD_AGGREGATIONS_KEY
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import KeywordManager
from keywords.api.serializers.keyword_with_views_history import KeywordWithViewsHistorySerializer
from keywords.constants import MATCH_PHRASE_FILTER
from keywords.constants import RANGE_FILTER
from keywords.constants import TERMS_FILTER
from keywords.utils import KeywordViralParamAdapter
from userprofile.constants import StaticPermissions
from utils.aggregation_constants import ALLOWED_KEYWORD_AGGREGATIONS
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter


class KeywordListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.RESEARCH),
    )
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)
    pagination_class = ResearchPaginator
    serializer_class = KeywordWithViewsHistorySerializer
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.top_category_last_30day_views:desc",
        "stats.top_category_last_7day_views:desc",
        "stats.top_category_last_day_views:desc",
        "stats.search_volume:desc",
        "stats.average_cpc:desc",
        "stats.competition:desc",
        "stats.views:desc",
        "stats.last_30day_views:asc",
        "stats.top_category_last_30day_views:asc",
        "stats.search_volume:asc",
        "stats.average_cpc:asc",
        "stats.competition:asc",
        "stats.views:asc",
        "stats.top_category_last_7day_views:asc",
        "stats.top_category_last_day_views:asc",
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    params_adapters = (KeywordViralParamAdapter,)

    allowed_aggregations = ALLOWED_KEYWORD_AGGREGATIONS

    allowed_percentiles = (
        "stats.search_volume:percentiles",
        "stats.average_cpc:percentiles",
        "stats.competition:percentiles",
    )

    try:
        cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=KEYWORD_AGGREGATIONS_KEY)
        cached_aggregations = cached_aggregations_object.value
    # pylint: disable=broad-except
    except Exception as e:
        # pylint: enable=broad-except
        cached_aggregations = None

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.STATS,)

        query_params = deepcopy(self.request.query_params)
        if query_params.get("from_channel"):
            channel_id = query_params.get("from_channel")
            channel = ChannelManager().model.get(channel_id, _source=(f"{Sections.GENERAL_DATA}.video_tags"))
            keyword_ids = list(channel.general_data.video_tags)

            if keyword_ids:
                # pylint: disable=protected-access
                self.request.query_params._mutable = True
                # pylint: enable=protected-access
                self.request.query_params["main.id"] = keyword_ids
                self.terms_filter = self.terms_filter + ("main.id",)

        return ESQuerysetAdapter(KeywordManager(sections), cached_aggregations=self.cached_aggregations)
