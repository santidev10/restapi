from rest_framework.views import APIView

from keywords.tasks.export_data import export_keywords_data
from utils.es_components_exporter import ESDataS3ExportApiView
from userprofile.constants import StaticPermissions


class KeywordListExportApiView(ESDataS3ExportApiView, APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.RESEARCH__EXPORT),
    )
    generate_export_task = export_keywords_data

    @staticmethod
    def get_filename(name):
        return f"Keywords export report {name}.csv"
