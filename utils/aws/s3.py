from .base_service import AWSService
from .base_service import get_client


def get_s3_client():
    return get_client(AWSService.S3)
