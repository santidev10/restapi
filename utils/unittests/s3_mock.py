from contextlib import contextmanager
from functools import wraps
from unittest import TestCase

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


class S3TestCaseMetaclass(type):
    def __new__(mcs, name, bases, attrs):
        for attr in attrs:
            value = attrs[attr]
            if callable(value) and attr.startswith("test_"):
                attrs[attr] = mock_s3(value)
        return type.__new__(mcs, name, bases, attrs)


class S3TestCase(TestCase, metaclass=S3TestCaseMetaclass):
    pass
