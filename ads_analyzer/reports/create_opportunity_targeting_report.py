import logging

from django.conf import settings

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from saas import celery_app
from utils.aws.s3_exporter import S3Exporter
from utils.views import XLSX_CONTENT_TYPE

logger = logging.getLogger(__name__)

S3_FILE_KEY_PATTERN = "opportunity_targeting_reports/{opportunity_id}_{date_from}_{date_to}"


@celery_app.task
def create_opportunity_targeting_report(opportunity_id: str, date_from_str: str, date_to_str: str):
    report_generator = OpportunityTargetingReportXLSXGenerator()
    report = report_generator.build(opportunity_id, date_from_str, date_to_str)

    export_cls = OpportunityTargetingReportS3Exporter
    file_key = export_cls.get_s3_key(opportunity_id, date_from_str, date_to_str)
    export_cls.export_to_s3(report, file_key)

    OpportunityTargetingReport.objects.filter(
        opportunity_id=opportunity_id,
        date_from=date_from_str,
        date_to=date_to_str,
    ) \
        .update(
        staus=ReportStatus.SUCCESS.value,
        s3_file_key=file_key,
    )


class OpportunityTargetingReportXLSXGenerator:

    def build(self, opportunity_id, date_from, date_to):
        return None


class OpportunityTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = XLSX_CONTENT_TYPE

    @staticmethod
    def get_s3_key(opportunity_id, date_from, date_to):
        key = S3_FILE_KEY_PATTERN.format(opportunity_id=opportunity_id, date_from=date_from, date_to=date_to)
        return key
