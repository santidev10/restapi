from django.urls import reverse
from django.conf import settings

from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from highlights.api.urls.names import HighlightsNames
from highlights.tasks.video_export import export_videos_data
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import ExportDataAllowed
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission

from saas.urls.namespaces import Namespace

class HighlightVideosExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            ExportDataAllowed,
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    generate_export_task = export_videos_data

    @staticmethod
    def get_filename(name):
        return f"Videos export report {name}.csv"

    def _get_url_to_export(self, export_name):
        return settings.HOST + reverse(
            "{}:{}".format(Namespace.HIGHLIGHTS,  HighlightsNames.VIDEOS_EXPORT),
            args=(export_name,)
        )
