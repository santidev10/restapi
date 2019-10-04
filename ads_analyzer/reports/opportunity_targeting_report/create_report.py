import logging
from io import BytesIO

import xlsxwriter
from django.db.models import Sum
from itertools import chain

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.opportunity_targeting_report.renderers import TargetSheetTableRenderer
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableKeywordSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableTopicSerializer
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Opportunity
from aw_reporting.models import TopicStatistic
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
from saas import celery_app

logger = logging.getLogger(__name__)


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
        target_models = (
            (TopicStatistic, ("topic_id", "topic__name"), TargetTableTopicSerializer),
            (KeywordStatistic, ("keyword",), TargetTableKeywordSerializer),
        )
        values_shared = (
            "ad_group__campaign__name",
            "ad_group__name",
            "ad_group__campaign__salesforce_placement__name",
            "ad_group__campaign__salesforce_placement__start",
            "ad_group__campaign__salesforce_placement__end",
            "ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over",
            "ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )

        def get_serializer(model, group_by, serializer):
            queryset = model.objects \
                .filter(ad_group__campaign__salesforce_placement__opportunity_id=opportunity_id,
                        date__gte=date_from,
                        date__lte=date_to, ) \
                .values(*group_by, *values_shared) \
                .order_by(*group_by) \
                .annotate(sum_impressions=Sum("impressions"),
                          sum_video_views=Sum("video_views"),
                          sum_clicks=Sum("clicks"),
                          sum_cost=Sum("cost"), )
            return serializer(queryset, many=True)

        serializers = [
            get_serializer(*args)
            for args in target_models
        ]
        data = chain(*[serializer.data for serializer in serializers])

        renderer = TargetSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(data)

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
