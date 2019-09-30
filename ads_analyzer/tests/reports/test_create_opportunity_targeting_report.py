from datetime import date
from io import BytesIO
from unittest.mock import patch

from django.db.models.signals import post_save
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

    def _act(self, opportunity_id, date_from, date_to):
        create_opportunity_targeting_report.si(
            opportunity_id=opportunity_id,
            date_from_str=str(date_from),
            date_to_str=str(date_to),
        ).apply_async()

    def _get_report_workbook(self, opportunity_id, date_from, date_to):
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(
            opportunity_id,
            str(date_from),
            str(date_to)
        )
        self.assertTrue(OpportunityTargetingReportS3Exporter.exists(s3_key, get_key=False))
        file = OpportunityTargetingReportS3Exporter.get_s3_export_content(s3_key, get_key=False)
        book = load_workbook(BytesIO(file.read()))
        return book

    @mock_s3
    def test_empty_report_updates_db_entity(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
            )
        self._act(opportunity.id, date_from, date_to)

        report.refresh_from_db()
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(opportunity.id, str(date_from), str(date_to))
        self.assertEqual(s3_key, report.s3_file_key)
        self.assertEqual(ReportStatus.SUCCESS.value, report.status)

    @mock_s3
    def test_empty_report_to_s3(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        self._act(opportunity.id, date_from, date_to)

        book = self._get_report_workbook(opportunity.id, date_from, date_to)
        self.assertIsNotNone(book)

    @mock_s3
    def test_empty_report_header(self):
        opportunity = Opportunity.objects.create(
            id=next(str_iterator),
            name="Test Opportunity name",
        )
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        self._act(opportunity.id, date_from, date_to)

        book = self._get_report_workbook(opportunity.id, date_from, date_to)
        self.assertEqual(
            ["Target", "Devices", "Demo", "Video"],
            book.sheetnames
        )
        for sheet, sheet_name in zip(book.worksheets, book.sheetnames):
            with self.subTest(sheet_name):
                self.assertEqual(
                    f"Opportunity: {opportunity.name}",
                    sheet.cell(None, 1, 1).value,
                )
                self.assertEqual(
                    f"Date Range: {date_from} - {date_to}",
                    sheet.cell(None, 2, 1).value,
                )
