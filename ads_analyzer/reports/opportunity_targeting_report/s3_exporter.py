from django.conf import settings

from utils.aws.s3_exporter import S3Exporter
from utils.views import XLSX_CONTENT_TYPE

S3_FILE_KEY_PATTERN = "opportunity_targeting_reports/{opportunity_id}_{date_from}_{date_to}_{created_at}"


class OpportunityTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = XLSX_CONTENT_TYPE

    @staticmethod
    def get_s3_key(report):
        key = S3_FILE_KEY_PATTERN.format(opportunity_id=report.opportunity_id,
                                         date_from=report.date_from,
                                         date_to=report.date_to,
                                         created_at=report.created_at)
        return key
