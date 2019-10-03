from django.conf import settings

from ads_analyzer.reports.opportunity_targeting_report.create_report import S3_FILE_KEY_PATTERN
from utils.aws.s3_exporter import S3Exporter
from utils.views import XLSX_CONTENT_TYPE


class OpportunityTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = XLSX_CONTENT_TYPE

    @staticmethod
    def get_s3_key(opportunity_id, date_from, date_to):
        key = S3_FILE_KEY_PATTERN.format(opportunity_id=opportunity_id, date_from=date_from, date_to=date_to)
        return key
