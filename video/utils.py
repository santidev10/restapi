from django.conf import settings

from utils.aws.s3_exporter import S3Exporter

S3_EXPORT_KEY_PATTERN = "export/{name}.csv"


class VideoListS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @staticmethod
    def get_s3_key(name):
        key = S3_EXPORT_KEY_PATTERN.format(name=name)
        return key