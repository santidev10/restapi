from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import VideoManager
from highlights.api.utils import HighlightsPaginator
from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from video.api.serializers.video_with_blacklist_data import VideoWithBlackListSerializer


class HighlightsVideosPaginator(HighlightsPaginator):
    def _get_response_data(self, data):
        response_data = super()._get_response_data(data)
        language_aggregation = response_data.get("aggregations", {}).get("general_data.language")
        if language_aggregation:
            language_aggregation["buckets"] = language_aggregation.get("buckets", [])[:10]
        return response_data


class HighlightVideosListApiView(APIViewMixin, ListAPIView):
    serializer_class = VideoWithBlackListSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )

    pagination_class = HighlightsVideosPaginator
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.last_7day_views:desc",
        "stats.last_day_views:desc",
    )

    terms_filter = (
        "general_data.category",
        "general_data.language",
    )
    allowed_aggregations = (
        "general_data.category",
        "general_data.language",
    )
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA,
                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION,
                    Sections.CAPTIONS, Sections.BRAND_SAFETY,)
        if self.request.user.is_staff or \
                self.request.user.has_perm("userprofile.video_audience"):
            sections += (Sections.ANALYTICS,)
        return ESQuerysetAdapter(VideoManager(sections))
