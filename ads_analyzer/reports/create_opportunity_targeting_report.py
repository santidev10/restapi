import logging
from io import BytesIO

import xlsxwriter
from django.conf import settings
from django.db.models import F
from rest_framework.fields import BooleanField
from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.fields import ReadOnlyField
from rest_framework.serializers import ModelSerializer
from rest_framework_csv.renderers import CSVRenderer

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from aw_reporting.models import Opportunity
from aw_reporting.models import TopicStatistic
from aw_reporting.models.salesforce_constants import goal_type_str
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
        sheet_headers = self._get_headers(opportunity_id, date_from, date_to)
        self._add_target_sheet(workbook, sheet_headers, opportunity_id, date_from, date_to)
        self._add_devices_sheet(workbook, opportunity_id, date_from, date_to)
        self._add_demo_sheet(workbook, opportunity_id, date_from, date_to)
        self._add_video_sheet(workbook, opportunity_id, date_from, date_to)

        workbook.close()
        output.seek(0)
        return output

    def _get_headers(self, opportunity_id, date_from, date_to):
        opportunity = Opportunity.objects.get(pk=opportunity_id)
        return [
            f"Opportunity: {opportunity.name}",
            f"Date Range: {date_from} - {date_to}"
        ]

    def _add_target_sheet(self, wb, sheet_headers, opportunity_id, date_from, date_to):
        queryset = TopicStatistic.objects.filter(
            ad_group__campaign__salesforce_placement__opportunity_id=opportunity_id,
            date__gte=date_from,
            date__lte=date_to,
        ).annotate(
            aa=F("ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over")
        )
        serializer = TargetTableSerializer(queryset, many=True)
        renderer = TargetSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(serializer.data)

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


class Cursor:
    def __init__(self):
        super().__init__()
        self.row, self.column = 0, 0

    def __str__(self):
        return f"Cursor <{self.row}, {self.column}>"

    def next_row(self, keep_column=False):
        self.row += 1

        if not keep_column:
            self.column = 0

    def next_column(self):
        self.column += 1


class SheetTableRenderer(CSVRenderer):
    sheet_name = None

    def __init__(self, workbook, sheet_headers=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workbook = workbook
        self.sheet_headers = sheet_headers or []
        self.cursor = Cursor()

    def render(self, data):
        sheet = self.workbook.add_worksheet(self.sheet_name)
        self._render_header(sheet)
        self._render_empty_line(sheet)
        self._render_table(sheet, data)

    def _render_header(self, sheet):
        cursor = self.cursor
        for row in self.sheet_headers:
            sheet.write(cursor.row, cursor.column, row)
            cursor.next_row()

    def _render_empty_line(self, sheet):
        self.cursor.next_row()

    def _render_table(self, sheet, data):
        table = self.tablize(data, header=self.header, labels=self.labels)
        cursor = self.cursor
        for row in table:
            for value in row:
                sheet.write(cursor.row, cursor.column, value)
                cursor.next_column()
            cursor.next_row()


class TargetSheetTableRenderer(SheetTableRenderer):
    sheet_name = "Target"
    header = [
        "name",
        "type",
        "campaign_name",
        "ad_group_name",
        "placement_name",
        "placement_start",
        "placement_end",
        "days_remaining",
        "margin_cap",
        "cannot_roll_over",
        "rate_type",
        "contracted_rate",
        "max_bid",
        "avg_rate",
        "cost",
        "cost_delivery_percentage",
        "impressions",
        "views",
        "delivery_percentage",
        "revenue",
        "profit",
        "margin",
        "video_played_to_100",
        "view_rate",
        "clicks",
        "ctr",
    ]
    labels = {
        "name": "Target",
        "type": "Type",
        "campaign_name": "Ads Campaign",
        "ad_group_name": "Ads Ad group",
        "placement_name": "Salesforce Placement",
        "placement_start": "Placement Start Date",
        "placement_end": "Placement End Date",
        "days_remaining": "Days remaining",
        "margin_cap": "Margin Cap",
        "cannot_roll_over": "Cannot Roll over Delivery",
        "rate_type": "Rate Type",
        "contracted_rate": "Contracted Rate",
        "max_bid": "Max bid",
        "avg_rate": "Avg. Rate",
        "cost": "Cost",
        "cost_delivery_percentage": "Cost delivery percentage",
        "impressions": "Impressions",
        "views": "Views",
        "delivery_percentage": "Delivery percentage",
        "revenue": "Revenue",
        "profit": "Profit",
        "margin": "Margin",
        "video_played_to_100": "Video played to 100%",
        "view_rate": "View rate",
        "clicks": "Clicks",
        "ctr": "CTR",
    }


class GoalTypeField(CharField):
    def to_representation(self, goal_type_id):
        goal_type = goal_type_str(goal_type_id)
        return super(GoalTypeField, self).to_representation(goal_type)


class TargetTableSerializer(ModelSerializer):
    name = CharField(source="topic.name")
    type = ReadOnlyField(default="Topic")
    campaign_name = CharField(source="ad_group.campaign.name")
    ad_group_name = CharField(source="ad_group.name")
    placement_name = CharField(source="ad_group.campaign.salesforce_placement.name")
    placement_start = DateField(source="ad_group.campaign.salesforce_placement.start")
    placement_end = DateField(source="ad_group.campaign.salesforce_placement.end")
    margin_cap = ReadOnlyField(default="N/A")
    cannot_roll_over = BooleanField(source="ad_group.campaign.salesforce_placement.opportunity.cannot_roll_over")
    rate_type = GoalTypeField(source="ad_group.campaign.salesforce_placement.goal_type_id")

    class Meta:
        model = TopicStatistic
        fields = (
            "name",
            "type",
            "campaign_name",
            "ad_group_name",
            "placement_name",
            "placement_start",
            "placement_end",
            "margin_cap",
            "cannot_roll_over",
            "rate_type",
        )
