from rest_framework.permissions import IsAdminUser
from rest_framework.views import APIView

from utils.api.mutate_query_params import mutate_query_params
from utils.api.mutate_query_params import VettingAdminAggregationsMixin
from utils.datetime import now_in_default_tz
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import BrandSafetyDataVisible
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from video.tasks.export_data import export_videos_data


class VideoListExportApiView(VettingAdminAggregationsMixin,ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.research_exports"),
            IsAdminUser
        ),
    )
    generate_export_task = export_videos_data

    def _get_query_params(self, request):
        if not BrandSafetyDataVisible().has_permission(request):

            if "brand_safety" in request.query_params:
                with mutate_query_params(request.query_params):
                    request.query_params["brand_safety"] = None

        if not self.request.user.has_perm("userprofile.transcripts_filter") and \
            not self.request.user.is_staff:
            if "transcripts" in self.request.query_params:
                with mutate_query_params(self.request.query_params):
                    self.request.query_params["transcripts"] = None

        self.guard_vetting_admin_aggregations()

        return super(VideoListExportApiView, self)._get_query_params(request)

    @staticmethod
    def get_filename(name):
        return f"Videos export report {name}.csv"

    @staticmethod
    def get_report_prefix():
        return f'research_videos_export_{now_in_default_tz().date().strftime("%Y_%m_%d")}'
