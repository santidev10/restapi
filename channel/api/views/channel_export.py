from django.conf import settings
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


class ChannelListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            ExportDataAllowed,
            user_has_permission("userprofile.channel_list"),
            IsAdminUser
        ),
    )
    generate_export_task = export_channels_data

    @staticmethod
    def get_filename(name):
        return f"Channels export report {name}.csv"


    def _get_url_to_export(self, export_name):
        return settings.HOST + reverse(
            "{}:{}".format(Namespace.CHANNEL, ChannelPathName.CHANNEL_LIST_EXPORT),
            args=(export_name,)
        )
