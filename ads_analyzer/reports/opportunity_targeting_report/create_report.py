import logging
from io import BytesIO

import xlsxwriter
from django.db.models import Q

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Opportunity
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.lang import merge_sort
from .renderers import DemoSheetTableRenderer
from .renderers import DevicesSheetTableRenderer
from .renderers import TargetSheetTableRenderer
from .renderers import VideosSheetTableRenderer
from .s3_exporter import OpportunityTargetingReportS3Exporter
from .serializers import DemoAgeRangeTableSerializer
from .serializers import DemoGenderTableSerializer
from .serializers import DevicesTableSerializer
from .serializers import TargetTableAudienceSerializer
from .serializers import TargetTableChannelSerializer
from .serializers import TargetTableKeywordSerializer
from .serializers import TargetTableTopicSerializer
from .serializers import TargetTableVideoSerializer
from .serializers import VideosTableSerializer

logger = logging.getLogger(__name__)


@celery_app.task(autoretry_for=(OpportunityTargetingReport.DoesNotExist,),
                 retry_kwargs={"max_retries": 5},
                 retry_backoff=True)
def create_opportunity_targeting_report(report_id):
    now = now_in_default_tz()
    report_entity = OpportunityTargetingReport.objects.get(pk=report_id)
    report_generator = OpportunityTargetingReportXLSXGenerator(now=now)
    report = report_generator.build(report_entity.opportunity_id, report_entity.date_from, report_entity.date_to)

    export_cls = OpportunityTargetingReportS3Exporter
    file_key = export_cls.get_s3_key(report_entity)
    export_cls.export_object_to_s3(report, file_key)
    report_queryset = OpportunityTargetingReport.objects.filter(
        pk=report_id, )
    report_queryset.update(
        status=ReportStatus.SUCCESS.value,
        s3_file_key=file_key,
    )
    notify_opportunity_targeting_report_is_ready.si(report_id=report_id) \
        .apply_async()


class OpportunityTargetingReportXLSXGenerator:
    def __init__(self, now):
        self.now = now

    def build(self, opportunity_id, date_from, date_to):
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {
            "in_memory": True,
        })
        sheet_headers = self._get_headers(opportunity_id, date_from, date_to)
        self._add_target_sheet(workbook, sheet_headers, opportunity_id, date_from, date_to)
        self._add_devices_sheet(workbook, sheet_headers, opportunity_id, date_from, date_to)
        self._add_demo_sheet(workbook, sheet_headers, opportunity_id, date_from, date_to)
        self._add_video_sheet(workbook, sheet_headers, opportunity_id, date_from, date_to)

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
            (TopicStatistic, TargetTableTopicSerializer),
            (KeywordStatistic, TargetTableKeywordSerializer),
            (YTChannelStatistic, TargetTableChannelSerializer),
            (YTVideoStatistic, TargetTableVideoSerializer),
            (AudienceStatistic, TargetTableAudienceSerializer),
        )
        filters = self._build_filters(opportunity_id, date_from, date_to)

        def get_serializer(model, serializer):
            queryset = model.objects.filter(filters)

            return serializer(queryset, many=True, context=dict(now=self.now))

        serializers = [
            get_serializer(*args)
            for args in target_models
        ]
        data = merge_sort([serializer.data for serializer in serializers], key=lambda i: -i["video_views"])

        renderer = TargetSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(data)

    def _add_devices_sheet(self, wb, sheet_headers, opportunity_id, date_from, date_to):
        queryset = AdGroupStatistic.objects.filter(self._build_filters(opportunity_id, date_from, date_to))
        serializer = DevicesTableSerializer(queryset, many=True, context=dict(now=self.now))
        data = serializer.data

        renderer = DevicesSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(data)

    def _add_demo_sheet(self, wb, sheet_headers, opportunity_id, date_from, date_to):
        demo_models = (
            (AgeRangeStatistic, DemoAgeRangeTableSerializer),
            (GenderStatistic, DemoGenderTableSerializer),
        )
        filters = self._build_filters(opportunity_id, date_from, date_to)

        def get_serializer(model, serializer):
            queryset = model.objects.filter(filters)

            return serializer(queryset, many=True, context=dict(now=self.now))

        serializers = [
            get_serializer(*args)
            for args in demo_models
        ]
        data = merge_sort([serializer.data for serializer in serializers], key=lambda i: -i["video_views"])

        renderer = DemoSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(data)

    def _add_video_sheet(self, wb, sheet_headers, opportunity_id, date_from, date_to):
        queryset = AdStatistic.objects.filter(self._build_filters(opportunity_id, date_from, date_to, "ad__ad_group"))
        serializer = VideosTableSerializer(queryset, many=True, context=dict(now=self.now))
        data = serializer.data

        renderer = VideosSheetTableRenderer(workbook=wb, sheet_headers=sheet_headers)
        renderer.render(data)

    def _build_filters(self, opportunity_id, date_from, date_to, ad_group_ref: str = "ad_group"):
        opportunity_id_ref = f"{ad_group_ref}__campaign__salesforce_placement__opportunity_id"
        filters = Q(**{opportunity_id_ref: opportunity_id})
        if date_from:
            filters = filters & Q(date__gte=date_from)
        if date_to:
            filters = filters & Q(date__lte=date_to)
        return filters

    def _add_sheet_header(self, sheet, opportunity_id, date_from, date_to):
        opportunity = Opportunity.objects.get(pk=opportunity_id)
        sheet.write(0, 0, f"Opportunity: {opportunity.name}")
        sheet.write(1, 0, f"Date Range: {date_from} - {date_to}")
