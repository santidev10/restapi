from django.conf import settings

from utils.aws.s3_exporter import S3Exporter
from utils.api.s3_export_api import S3ExportApiView

S3_EXPORT_KEY_PATTERN = "exported_files/{name}.csv"


class ESDataS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @staticmethod
    def get_s3_key(name):
        key = S3_EXPORT_KEY_PATTERN.format(name=name)
        return key


class ESDataS3ExportApiView(S3ExportApiView):
    s3_exporter = ESDataS3Exporter
