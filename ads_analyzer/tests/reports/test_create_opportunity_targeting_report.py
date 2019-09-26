from datetime import date

from django.test import TransactionTestCase
from openpyxl import load_workbook

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.create_opportunity_targeting_report import OpportunityTargetingReportS3Exporter
from ads_analyzer.tasks import create_opportunity_targeting_report
from aw_reporting.models import Opportunity
from utils.utittests.s3_mock import mock_s3
from utils.utittests.str_iterator import str_iterator


class CreateOpportunityTargetingReportTestCase(TransactionTestCase):

    @mock_s3
    def test_empty_report_updates_db_entity(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)
        report = OpportunityTargetingReport.objects.create(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to,
        )
        create_opportunity_targeting_report.si(
            opportunity_id=opportunity.id,
            date_from_str=str(date_from),
            date_to_str=str(date_to),
        ).apply_async()

        report.refresh_from_db()
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(opportunity.id, str(date_from), str(date_to))
        self.assertEqual(s3_key, report.s3_file_key)
        self.assertEqual(ReportStatus.SUCCESS.value, report.status)

    @mock_s3
    def test_empty_report_content(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)
        OpportunityTargetingReport.objects.create(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to,
        )

        create_opportunity_targeting_report.si(
            opportunity_id=opportunity.id,
            date_from_str=str(date_from),
            date_to_str=str(date_to),
        ).apply_async()

        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(opportunity.id, str(date_from), str(date_to))
        self.assertTrue(OpportunityTargetingReportS3Exporter.exists(s3_key, get_key=False))
        report = OpportunityTargetingReportS3Exporter.get_s3_export_content(s3_key)
        book = load_workbook(report)
        print(book)
