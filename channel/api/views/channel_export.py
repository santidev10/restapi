from django.urls import reverse

from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from channel.tasks.export_data import export_channels_data
from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import ExportDataAllowed
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.permissions import BrandSafetyDataVisible


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
                request.query_params._mutable = True
                request.query_params["brand_safety"] = None
                request.query_params._mutable = False

        return super(ChannelListExportApiView, self)._get_query_params(request)

    @staticmethod
    def get_filename(name):
        return f"Channels export report {name}.csv"

    def _get_url_to_export(self, export_name):
        host_link = self.get_host_link(self.request)
        return host_link + reverse(
            "{}:{}".format(Namespace.CHANNEL, ChannelPathName.CHANNEL_LIST_EXPORT),
            args=(export_name,)
        )
