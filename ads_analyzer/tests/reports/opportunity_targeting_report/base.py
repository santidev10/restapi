from datetime import date
from datetime import timedelta
from io import BytesIO
from unittest.mock import patch

from django.db.models.signals import post_save
from django.test import TransactionTestCase
from openpyxl import load_workbook

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.reports.opportunity_targeting_report.create_report import create_opportunity_targeting_report
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from utils.utittests.s3_mock import S3TestCase
from utils.utittests.str_iterator import str_iterator


class CreateOpportunityTargetingReportBaseTestCase(TransactionTestCase, S3TestCase):
    def act(self, opportunity_id, date_from, date_to):
        with patch.object(post_save, "send"):
            report, _ = OpportunityTargetingReport.objects.get_or_create(
                opportunity_id=opportunity_id,
                date_from=date_from,
                date_to=date_to,
            )
        create_opportunity_targeting_report(
            report_id=report.id,
        )
        return report

    def get_report_workbook(self, opportunity_id, date_from, date_to):
        report = OpportunityTargetingReport.objects.get(
            opportunity_id=opportunity_id,
            date_from=date_from,
            date_to=date_to,
        )
        s3_key = OpportunityTargetingReportS3Exporter.get_s3_key(report)
        self.assertTrue(OpportunityTargetingReportS3Exporter.exists(s3_key, get_key=False))
        file = OpportunityTargetingReportS3Exporter.get_s3_export_content(s3_key, get_key=False)
        book = load_workbook(BytesIO(file.read()))
        return book


class ColumnsDeclaration:
    def __init__(self, definition):
        self.definition = definition
        self.values = dict(definition)

    def __getattr__(self, item):
        try:
            return super().__getattribute__(item)
        except AttributeError:
            return self.values[item]

    def labels(self):
        return tuple([name for _, name in self.definition])

    def ids(self):
        return tuple([name for name, _ in self.definition])


class CreateOpportunityTargetingReportSheetTestCase(CreateOpportunityTargetingReportBaseTestCase):
    SHEET_NAME = None
    DATA_ROW_INDEX = 3
    columns: ColumnsDeclaration = None

    def get_data_table(self, opportunity_id, date_from, date_to):
        book = self.get_report_workbook(opportunity_id, date_from, date_to)
        sheet = book.get_sheet_by_name(self.SHEET_NAME)
        return list(sheet)[self.DATA_ROW_INDEX:]

    def setUp(self) -> None:
        super().setUp()
        self.opportunity = Opportunity.objects.create(
            id=next(str_iterator),
            name="Test opportunity #123",
            margin_cap_required=True,
        )

        any_date = date(2019, 1, 1)
        pl_start, pl_end = any_date - timedelta(days=1), any_date + timedelta(days=1)
        self.placement = OpPlacement.objects.create(opportunity=self.opportunity, name="Test Placement",
                                                    goal_type_id=SalesForceGoalType.CPV, ordered_rate=1.23,
                                                    start=pl_start, end=pl_end)
        self.campaign = Campaign.objects.create(salesforce_placement=self.placement, name="Test Campaign")
        self.ad_group = AdGroup.objects.create(campaign=self.campaign, name="Test AdGroup")
