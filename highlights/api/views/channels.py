from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import ChannelManager
from highlights.api.utils import HighlightsPaginator
from utils.api.research import ESBrandSafetyFilterBackend
from utils.api.research import ESQuerysetWithBrandSafetyAdapter
from utils.es_components_api_utils import APIViewMixin
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightChannelsListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    pagination_class = HighlightsPaginator
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
    filter_backends = (ESBrandSafetyFilterBackend,)

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL,)
        if self.request.user.is_staff:
            sections += (Sections.ANALYTICS,)
        return ESQuerysetWithBrandSafetyAdapter(ChannelManager(sections), max_items=100)
