from django.urls import reverse
from django.conf import settings

from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from video.tasks.export_data import export_videos_data
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.permissions import ExportDataAllowed
from video.api.urls.names import Name
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames


class VideoListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            ExportDataAllowed,
            user_has_permission("userprofile.video_list"),
            IsAdminUser
        ),
    )
    generate_export_task = export_videos_data

    def _get_query_params(self, request):
        if not (request.user.is_staff or request.user.has_perm("userprofile.scoring_brand_safety") or
                request.user.has_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)):

            if "brand_safety" in request.query_params:
                request.query_params._mutable = True
                request.query_params["brand_safety"] = None
                request.query_params._mutable = False

        return super(VideoListExportApiView, self)._get_query_params(request)

    @staticmethod
    def get_filename(name):
        return f"Videos export report {name}.csv"

    def _get_url_to_export(self, export_name):
        return settings.HOST + reverse("{}:{}".format(Namespace.VIDEO, Name.VIDEO_EXPORT), args=(export_name,))
