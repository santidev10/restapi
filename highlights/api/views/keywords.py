from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import KeywordManager
from highlights.api.utils import HighlightsPaginator
from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import ESSerializer
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightKeywordsListApiView(ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    serializer_class = ESSerializer
    pagination_class = HighlightsPaginator
    queryset = ESQuerysetAdapter(KeywordManager(Sections.STATS), max_items=100)
    filter_backends = (FreeFieldOrderingFilter,)
    ordering_fields = ("stats.last_30day_views:desc",)
