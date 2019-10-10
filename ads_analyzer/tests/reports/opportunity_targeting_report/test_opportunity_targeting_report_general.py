from datetime import date
from unittest.mock import patch

from django.db.models.signals import post_save

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from aw_reporting.models import Opportunity
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
from saas import celery_app
from utils.utittests.celery import mock_send_task
from utils.utittests.str_iterator import str_iterator
from .base import CreateOpportunityTargetingReportBaseTestCase


class CreateOpportunityTargetingReportGeneralTestCase(CreateOpportunityTargetingReportBaseTestCase):
    def test_empty_report_updates_db_entity(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
            )
        self.act(opportunity.id, date_from, date_to)

        report.refresh_from_db()
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(report)
        report_file_extention = ".xlsx"

        self.assertEqual(s3_key, report.s3_file_key)
        self.assertIn(report_file_extention, s3_key)
        self.assertEqual(ReportStatus.SUCCESS.value, report.status)

    def test_send_email_notifications(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        with mock_send_task():
            report = self.act(opportunity.id, date_from, date_to)

            calls = celery_app.send_task.mock_calls

        self.assertEqual(1, len(calls))
        expected_kwargs = dict(
            report_id=report.id,
        )
        self.assertEqual(
            (notify_opportunity_targeting_report_is_ready.name, (), expected_kwargs),
            calls[0][1]
        )

    def test_empty_report_to_s3(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        self.act(opportunity.id, date_from, date_to)

        book = self.get_report_workbook(opportunity.id, date_from, date_to)
        self.assertIsNotNone(book)

    def test_empty_report_header(self):
        opportunity = Opportunity.objects.create(
            id=next(str_iterator),
            name="Test Opportunity name",
        )
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        self.act(opportunity.id, date_from, date_to)

        book = self.get_report_workbook(opportunity.id, date_from, date_to)
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
