from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from channel.api.serializers.channel_with_blacklist_data import ChannelWithBlackListSerializer
from es_components.constants import Sections
from es_components.managers import ChannelManager
from highlights.api.utils import HighlightsPaginator
from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightsChannelsPaginator(HighlightsPaginator):
    def _get_response_data(self, data):
        response_data = super()._get_response_data(data)
        language_aggregation = response_data.get("aggregations", {}).get("general_data.top_language")
        if language_aggregation:
            language_aggregation["buckets"] = language_aggregation.get("buckets", [])[:10]
        return response_data


class HighlightChannelsListApiView(APIViewMixin, ListAPIView):
    serializer_class = ChannelWithBlackListSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    pagination_class = HighlightsChannelsPaginator
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.last_7day_views:desc",
        "stats.last_day_views:desc",
    )
    terms_filter = (
        "general_data.top_category",
        "general_data.top_language",
    )
    allowed_aggregations = (
        "general_data.top_category",
        "general_data.top_language",
    )
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY,)
        if self.request.user.is_staff:
            sections += (Sections.ANALYTICS,)
        return ESQuerysetAdapter(ChannelManager(sections))
