from django.urls import reverse
from django.conf import settings

from rest_framework.permissions import IsAdminUser

from highlights.tasks.channel_export import export_channels_data
from highlights.api.urls.names import HighlightsNames
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.api.file_list_api_view import FileListApiView
from utils.permissions import ExportDataAllowed
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from saas.urls.namespaces import Namespace


class HighlightChannelsExportApiView(ESDataS3ExportApiView, FileListApiView):
    permission_classes = (
        or_permission_classes(
            ExportDataAllowed,
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    generate_export_task = export_channels_data

    @staticmethod
    def get_filename(name):
        return f"Channels export report {name}.csv"


    def _get_url_to_export(self, export_name):
        return settings.HOST + reverse(
        "{}:{}".format(Namespace.HIGHLIGHTS,  HighlightsNames.CHANNELS_EXPORT),
        args=(export_name,)
    )
