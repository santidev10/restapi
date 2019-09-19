from copy import deepcopy

from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import KeywordManager
from keywords.constants import TERMS_FILTER
from keywords.constants import RANGE_FILTER
from keywords.constants import MATCH_PHRASE_FILTER
from keywords.api.serializers.keyword_with_views_history import KeywordWithViewsHistorySerializer
from utils.api.filters import FreeFieldOrderingFilter
from utils.api.research import ResearchPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class KeywordListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_list"),
            IsAdminUser
        ),
    )
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)
    pagination_class = ResearchPaginator
    serializer_class = KeywordWithViewsHistorySerializer
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.top_category_last_30day_views:desc",
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
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER

    allowed_aggregations = (
        "stats.search_volume:min",
        "stats.search_volume:max",
        "stats.average_cpc:min",
        "stats.average_cpc:max",
        "stats.competition:min",
        "stats.competition:max",
        "stats.is_viral"
    )

    allowed_percentiles = (
        "stats.search_volume:percentiles",
        "stats.average_cpc:percentiles",
        "stats.competition:percentiles",
    )

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.STATS,)

        query_params = deepcopy(self.request.query_params)
        if query_params.get("from_channel"):
            channel_id = query_params.get("from_channel")
            channel = ChannelManager().model.get(channel_id, _source=(f"{Sections.GENERAL_DATA}.video_tags"))
            keyword_ids = list(channel.general_data.video_tags)

            if keyword_ids:
                self.request.query_params._mutable = True
                self.request.query_params["main.id"] = keyword_ids
                self.terms_filter = self.terms_filter + ("main.id",)

        if query_params.get("stats.is_viral"):
            if query_params.get("stats.is_viral") == "Viral":
                self.request.query_params._mutable = True
                self.request.query_params["stats.is_viral"] = "true"
            elif query_params.get("stats.is_viral") == "All":
                self.request.query_params._mutable = True
                self.request.query_params["stats.is_viral"] = ""

        return ESQuerysetAdapter(KeywordManager(sections))
