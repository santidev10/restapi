from abc import ABC, abstractmethod

import boto3

from django.conf import settings


class ReportNotFoundException(Exception):
    pass


class S3Exporter(ABC):
    bucket_name = settings.AMAZON_S3_BUCKET_NAME
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    @staticmethod
    @abstractmethod
    def get_s3_key(*args, **kwargs):
        pass

    @classmethod
    def _s3(cls):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=cls.aws_access_key_id,
            aws_secret_access_key=cls.aws_secret_access_key
        )
        return s3

    @classmethod
    def export_to_s3(cls, content_exporter, name):
        with content_exporter as exported_file_name:
            S3Exporter._s3().upload_file(
                Bucket=cls.bucket_name,
                Key=cls.get_s3_key(name),
                Filename=exported_file_name,
            )

    @classmethod
    def get_s3_export_content(cls, name):
        s3 = S3Exporter._s3()
        try:
            s3_object = s3.get_object(
                Bucket=cls.bucket_name,
                Key=cls.get_s3_key(name)
            )
        except s3.exceptions.NoSuchKey:
            raise ReportNotFoundException()
        body = s3_object.get("Body")
        return body
