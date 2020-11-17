from django.conf import settings

from utils.aws.s3_exporter import S3Exporter
from uuid import uuid4


class PerformS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_BUCKET_NAME
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    def get_s3_key(self):
        return f"{uuid4()}.csv"

    def export_file(self, filepath, display_filename):
        s3_key = self.get_s3_key()
        content_disposition = self.get_content_disposition(display_filename)
        self.export_file_to_s3(filepath, s3_key, extra_args=dict(ContentDisposition=content_disposition))
        return s3_key

    def export_file_to_s3(self, filename, s3_key, extra_args=None):
        extra_args = extra_args or {}
        self._s3().upload_file(
            Bucket=self.bucket_name,
            Key=s3_key,
            Filename=filename,
            ExtraArgs=extra_args,
        )

    def get_content_disposition(self, filename):
        content_disposition = 'attachment;filename="{filename}"'.format(filename=filename)
        return content_disposition
