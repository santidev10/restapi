from datetime import date
from datetime import timedelta
from io import BytesIO
from unittest.mock import ANY
from unittest.mock import patch

from django.db.models.signals import post_save
from django.test import TransactionTestCase
from openpyxl import load_workbook

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from ads_analyzer.tasks import create_opportunity_targeting_report
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
from saas import celery_app
from utils.utittests.celery import mock_send_task
from utils.utittests.s3_mock import S3TestCase
from utils.utittests.str_iterator import str_iterator


class CreateOpportunityTargetingReportBaseTestCase(TransactionTestCase, S3TestCase):
    def act(self, opportunity_id, date_from, date_to):
        create_opportunity_targeting_report(
            opportunity_id=opportunity_id,
            date_from_str=str(date_from),
            date_to_str=str(date_to),
        )

    def get_report_workbook(self, opportunity_id, date_from, date_to):
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(
            opportunity_id,
            str(date_from),
            str(date_to)
        )
        self.assertTrue(OpportunityTargetingReportS3Exporter.exists(s3_key, get_key=False))
        file = OpportunityTargetingReportS3Exporter.get_s3_export_content(s3_key, get_key=False)
        book = load_workbook(BytesIO(file.read()))
        return book


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
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(opportunity.id, str(date_from), str(date_to))
        self.assertEqual(s3_key, report.s3_file_key)
        self.assertEqual(ReportStatus.SUCCESS.value, report.status)

    def test_send_email_notifications(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator))
        date_from, date_to = date(2020, 1, 1), date(2020, 1, 2)

        with mock_send_task():
            self.act(opportunity.id, date_from, date_to)

            calls = celery_app.send_task.mock_calls

        self.assertEqual(1, len(calls))
        expected_kwargs = dict(
            opportunity_id=opportunity.id,
            date_from_str=str(date_from),
            date_to_str=str(date_to),
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


class CreateOpportunityTargetingReportSheetTestCase(CreateOpportunityTargetingReportBaseTestCase):
    SHEET_NAME = None
    DATA_ROW_INDEX = 3
    columns = None

    def get_data_table(self, opportunity_id, date_from, date_to):
        book = self.get_report_workbook(opportunity_id, date_from, date_to)
        sheet = book.get_sheet_by_name(self.SHEET_NAME)
        return list(sheet)[self.DATA_ROW_INDEX:]

    def setUp(self) -> None:
        super().setUp()
        self.opportunity = Opportunity.objects.create(
            id=next(str_iterator),
            name="Test opportunity #123",
        )


class CreateOpportunityTargetingReportTargetTestCase(CreateOpportunityTargetingReportSheetTestCase):
    SHEET_NAME = "Target"
    columns = (
        "Target",
        "Type",
        "Ads Campaign",
        "Ads Ad group",
        "Salesforce Placement",
        "Placement Start Date",
        "Placement End Date",
        "Days remaining",
        "Margin Cap",
        "Cannot Roll over Delivery",
        "Rate Type",
        "Contracted Rate",
        "Max bid",
        "Avg. Rate",
        "Cost",
        "Cost delivery percentage",
        "Impressions",
        "Views",
        "Delivery percentage",
        "Revenue",
        "Profit",
        "Margin",
        "Video played to 100%",
        "View rate",
        "Clicks",
        "CTR",
    )

    def test_headers(self):
        any_date = date(2019, 1, 1)
        self.act(self.opportunity.id, any_date, any_date)

        rows = self.get_data_table(self.opportunity.id, any_date, any_date)
        headers = rows[0]
        self.assertEqual(
            list(self.columns),
            [cell.value for cell in headers]
        )

    def test_topic_general_data(self):
        any_date = date(2019, 1, 1)
        pl_start, pl_end = any_date - timedelta(days=1), any_date + timedelta(days=1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()
        placement = OpPlacement.objects.create(opportunity=self.opportunity, name="Test Placement",
                                               goal_type_id=SalesForceGoalType.CPV,
                                               start=pl_start, end=pl_end)
        campaign = Campaign.objects.create(salesforce_placement=placement, name="Test Campaign")
        ad_group = AdGroup.objects.create(campaign=campaign, name="Test AdGroup")
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=ad_group, topic=topic, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        rows = self.get_data_table(self.opportunity.id, any_date, any_date)
        data = rows[1:]
        self.assertEqual(
            1,
            len(data)
        )
        data_row = data[0]
        data_values = [cell.value for cell in data_row]
        expected_values = [
            topic.name,
            "Topic",
            campaign.name,
            ad_group.name,
            placement.name,
            str(pl_start),
            str(pl_end),
            ANY,
            # FIXME: ADD "Margin Cap"
            "N/A",
            self.opportunity.cannot_roll_over,
            placement.goal_type
        ]
        self.assertEqual(
            expected_values,
            data_values[:11]
        )

    def test_keyword_general_data(self):
        any_date = date(2019, 1, 1)
        pl_start, pl_end = any_date - timedelta(days=1), any_date + timedelta(days=1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()
        placement = OpPlacement.objects.create(opportunity=self.opportunity, name="Test Placement",
                                               goal_type_id=SalesForceGoalType.CPV,
                                               start=pl_start, end=pl_end)
        campaign = Campaign.objects.create(salesforce_placement=placement, name="Test Campaign")
        ad_group = AdGroup.objects.create(campaign=campaign, name="Test AdGroup")
        keyword = "test keyword"
        KeywordStatistic.objects.create(keyword=keyword, ad_group=ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        rows = self.get_data_table(self.opportunity.id, any_date, any_date)
        data = rows[1:]
        self.assertEqual(
            1,
            len(data)
        )
        data_row = data[0]
        data_values = [cell.value for cell in data_row]
        expected_values = [
            keyword,
            "Keyword",
            campaign.name,
            ad_group.name,
            placement.name,
            str(pl_start),
            str(pl_end),
            ANY,
            # FIXME: ADD "Margin Cap"
            "N/A",
            self.opportunity.cannot_roll_over,
            placement.goal_type
        ]
        self.assertEqual(
            expected_values,
            data_values[:11]
        )
