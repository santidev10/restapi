from django.conf import settings

from utils.aws.s3_exporter import S3Exporter
from uuid import uuid4


class AccountTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME

    @staticmethod
    def get_s3_key(account_name):
        key = f"account_targeting_report/{account_name}_{uuid4()}.csv"
        return key
