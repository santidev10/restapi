from django.conf import settings

from utils.aws.s3_exporter import S3Exporter


class BlocklistExporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_BUCKET_NAME

    def get_s3_key(*args, **kwargs):
        raise NotImplementedError

    @classmethod
    def export_object_to_s3(cls, file_obj, file_key, extra_args=None):
        extra_args = extra_args or {}
        S3Exporter._s3().upload_fileobj(
            Fileobj=file_obj,
            Bucket=cls.bucket_name,
            Key=file_key,
            ExtraArgs=extra_args,
        )