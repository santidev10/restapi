from rest_framework.permissions import IsAdminUser
from rest_framework.views import APIView

from channel.tasks.export_data import export_channels_data
from utils.datetime import now_in_default_tz
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import BrandSafetyDataVisible
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class ChannelListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.research_exports"),
            IsAdminUser
        ),
    )
    generate_export_task = export_channels_data

    def _get_query_params(self, request):
        if not BrandSafetyDataVisible().has_permission(request):

            if "brand_safety" in request.query_params:
                # pylint: disable=protected-access
                request.query_params._mutable = True
                request.query_params["brand_safety"] = None
                request.query_params._mutable = False
                # pylint: enable=protected-access

        return super(ChannelListExportApiView, self)._get_query_params(request)

    @staticmethod
    def get_filename(name):
        return f"Channels export report {name}.csv"

    @staticmethod
    def get_report_prefix():
        return f'research_channels_export_{now_in_default_tz().date().strftime("%Y_%m_%d")}'
