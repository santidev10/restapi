import logging

from django.conf import settings

from saas import celery_app
from utils.aws.s3_exporter import S3Exporter
from utils.views import XLSX_CONTENT_TYPE

logger = logging.getLogger(__name__)

S3_FILE_KEY_PATTERN = "opportunity_targeting_reports/{opportunity_id}_{date_from}_{date_to}"


@celery_app.task
def create_opportunity_targeting_report(opportunity_id: str, date_from_str: str, date_to_str: str):
    pass


class OpportunityTargetingReportXLSXExporter:
    pass


class OpportunityTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = XLSX_CONTENT_TYPE

    @staticmethod
    def get_s3_key(opportunity_id, date_from, date_to):
        key = S3_FILE_KEY_PATTERN.format(opportunity_id=opportunity_id, date_from=date_from, date_to=date_to)
        return key
