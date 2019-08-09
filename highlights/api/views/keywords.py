from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import KeywordManager
from highlights.api.utils import HighlightsPaginator
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightKeywordsListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    pagination_class = HighlightsPaginator
    queryset = ESQuerysetAdapter(KeywordManager(Sections.STATS))
    ordering_fields = (
        "stats.top_category_last_30day_views:desc",
        "stats.top_category_last_7day_views:desc",
        "stats.top_category_last_day_views:desc",
    )

    terms_filter = ("stats.top_category",)
    allowed_aggregations = ("stats.top_category",)
