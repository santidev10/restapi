from contextlib import contextmanager
from functools import wraps

from django.conf import settings
from moto import mock_s3 as moto_mock_s3

from utils.aws.s3 import get_s3_client

__all__ = ["mock_s3"]


@contextmanager
def with_default_bucket():
    s3 = get_s3_client()
    s3.create_bucket(Bucket=settings.AMAZON_S3_BUCKET_NAME)
    s3.create_bucket(Bucket=settings.AMAZON_S3_REPORTS_BUCKET_NAME)
    yield


def mock_s3(fn):
    @moto_mock_s3
    @wraps(fn)
    def wrapper(*args, **kwargs):
        with with_default_bucket():
            return fn(*args, **kwargs)

    return wrapper
