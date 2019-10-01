import logging
from io import BytesIO

import xlsxwriter
from django.conf import settings

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from aw_reporting.models import Opportunity
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
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
    export_cls.export_object_to_s3(report, file_key)
    report_queryset = OpportunityTargetingReport.objects.filter(
        opportunity_id=opportunity_id,
        date_from=date_from_str,
        date_to=date_to_str,
    )
    report_queryset.update(
        status=ReportStatus.SUCCESS.value,
        s3_file_key=file_key,
    )
    notify_opportunity_targeting_report_is_ready.si(
        opportunity_id=opportunity_id,
        date_from_str=date_from_str,
        date_to_str=date_to_str,
    ) \
        .apply_async()


class OpportunityTargetingReportXLSXGenerator:

    def build(self, opportunity_id, date_from, date_to):
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {
            "in_memory": True,
        })
        self._add_target_sheet(workbook, opportunity_id, date_from, date_to)
        self._add_devices_sheet(workbook, opportunity_id, date_from, date_to)
        self._add_demo_sheet(workbook, opportunity_id, date_from, date_to)
        self._add_video_sheet(workbook, opportunity_id, date_from, date_to)

        workbook.close()
        output.seek(0)
        return output

    def _add_target_sheet(self, wb, opportunity_id, date_from, date_to):
        sheet = wb.add_worksheet("Target")
        self._add_sheet_header(sheet, opportunity_id, date_from, date_to)

    def _add_devices_sheet(self, wb, opportunity_id, date_from, date_to):
        sheet = wb.add_worksheet("Devices")
        self._add_sheet_header(sheet, opportunity_id, date_from, date_to)

    def _add_demo_sheet(self, wb, opportunity_id, date_from, date_to):
        sheet = wb.add_worksheet("Demo")
        self._add_sheet_header(sheet, opportunity_id, date_from, date_to)

    def _add_video_sheet(self, wb, opportunity_id, date_from, date_to):
        sheet = wb.add_worksheet("Video")
        self._add_sheet_header(sheet, opportunity_id, date_from, date_to)

    def _add_sheet_header(self, sheet, opportunity_id, date_from, date_to):
        opportunity = Opportunity.objects.get(pk=opportunity_id)
        sheet.write(0, 0, f"Opportunity: {opportunity.name}")
        sheet.write(1, 0, f"Date Range: {date_from} - {date_to}")


class OpportunityTargetingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = XLSX_CONTENT_TYPE

    @staticmethod
    def get_s3_key(opportunity_id, date_from, date_to):
        key = S3_FILE_KEY_PATTERN.format(opportunity_id=opportunity_id, date_from=date_from, date_to=date_to)
        return key
