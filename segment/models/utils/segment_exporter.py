from django.conf import settings

from utils.aws.s3_exporter import S3Exporter


class SegmentExporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_BUCKET_NAME
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    def get_s3_key(self):
        raise NotImplementedError("Method should be defined segment models")

    def set_bucket(self, bucket_name):
        self.bucket_name = bucket_name
