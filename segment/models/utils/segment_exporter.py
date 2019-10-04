from django.conf import settings

from segment.models.utils.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter


class SegmentExporter(S3Exporter):
    S3Exporter.bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    def __init__(self, bucket_name=None):
        S3Exporter.bucket_name = bucket_name or S3Exporter.bucket_name

    def get_s3_key(self):
        raise NotImplementedError("Method should be defined segment models")

    def export_to_s3(self, segment, s3_key, queryset=None):
        with ExportContextManager(segment=segment, queryset=queryset) as exported_file_name:
            self._s3().upload_file(
                Bucket=self.bucket_name,
                Key=s3_key,
                Filename=exported_file_name,
            )
