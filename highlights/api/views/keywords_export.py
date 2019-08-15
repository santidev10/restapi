from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers import KeywordManager
from highlights.api.views.keywords import ORDERING_FIELDS
from highlights.api.views.keywords import TERMS_FILTER
from keywords.api.views.keyword_export import KeywordCSVRendered
from keywords.api.views.keyword_export import KeywordListExportSerializer
from utils.api.file_list_api_view import FileListApiView
from utils.api.filters import FreeFieldOrderingFilter
from utils.datetime import time_instance
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightKeywordsExportApiView(APIViewMixin, FileListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    serializer_class = KeywordListExportSerializer
    renderer_classes = (KeywordCSVRendered,)
    ordering_fields = ORDERING_FIELDS
    terms_filter = TERMS_FILTER
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    @property
    def filename(self):
        now = time_instance.now()
        return "Keywords export report {}.csv".format(now.strftime("%Y-%m-%d_%H-%m"))

    def get_queryset(self):
        sections = (Sections.STATS,)
        return ESQuerysetAdapter(KeywordManager(sections)) \
            .order_by(ORDERING_FIELDS[0]) \
            .with_limit(100)
