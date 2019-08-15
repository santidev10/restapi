from rest_framework.permissions import IsAdminUser

from channel.api.views.channel_export import ChannelCSVRendered
from channel.api.views.channel_export import ChannelListExportSerializer
from es_components.constants import Sections
from es_components.managers import ChannelManager
from highlights.api.views.channels import ORDERING_FIELDS
from highlights.api.views.channels import TERMS_FILTERS
from utils.api.file_list_api_view import FileListApiView
from utils.api.filters import FreeFieldOrderingFilter
from utils.datetime import time_instance
from utils.es_components_api_utils import APIViewMixin
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightChannelsExportApiView(APIViewMixin, FileListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    serializer_class = ChannelListExportSerializer
    renderer_classes = (ChannelCSVRendered,)
    ordering_fields = ORDERING_FIELDS
    terms_filter = TERMS_FILTERS
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    @property
    def filename(self):
        now = time_instance.now()
        return "Channels export report {}.csv".format(now.strftime("%Y-%m-%d_%H-%m"))

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS, Sections.BRAND_SAFETY,)
        return ESQuerysetAdapter(ChannelManager(sections)).order_by(ORDERING_FIELDS[0]).with_limit(100)
