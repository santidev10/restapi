import boto3
from django.conf import settings


def get_client(service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY
    )


class AWSService:
    S3 = "s3"
