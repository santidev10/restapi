from django.urls import reverse
from django.conf import settings

from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from keywords.tasks.export_data import export_keywords_data
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import ExportDataAllowed
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from keywords.api.names import KeywordPathName

from saas.urls.namespaces import Namespace


class KeywordListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.research_exports"),
            IsAdminUser
        ),
    )
    generate_export_task = export_keywords_data

    @staticmethod
    def get_filename(name):
        return f"Keywords export report {name}.csv"
