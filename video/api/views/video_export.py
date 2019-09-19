from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from video.tasks.export_data import export_videos_data
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.permissions import ExportDataAllowed


class VideoListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            ExportDataAllowed,
            user_has_permission("userprofile.video_list"),
            IsAdminUser
        ),
    )
    generate_export_task = export_videos_data

    @staticmethod
    def get_filename(name):
        return f"Videos export report {name}.csv"
