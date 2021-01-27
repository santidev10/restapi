from datetime import timedelta

from django.conf import settings
from rest_framework.filters import BaseFilterBackend
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportModelSerializer
from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportPayloadSerializer
from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from userprofile.constants import StaticPermissions
from utils.api_paginator import CustomPageNumberPaginator
from utils.datetime import now_in_default_tz


class Paginator(CustomPageNumberPaginator):
    page_size = 10


class FilterBackend(BaseFilterBackend):
    def filter_queryset(self, request, queryset, view):
        if not request.user.has_permission(StaticPermissions.ADS_ANALYZER__RECIPIENTS):
            return queryset.filter(recipients=request.user)

        recipients = request.query_params.dict().get("recipients")
        if recipients:
            queryset = queryset.filter(recipients__in=recipients.split(","))

        return queryset


class OpportunityTargetingReportAPIView(ListCreateAPIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.ADS_ANALYZER),
    )
    serializer_class = OpportunityTargetReportModelSerializer
    pagination_class = Paginator
    filter_backends = (FilterBackend,)

    def get_queryset(self):
        return OpportunityTargetingReport.objects.filter(
            created_at__gte=self.get_visible_datetime()) \
            .order_by("-created_at")

    def post(self, request, *args, **kwargs):
        opportunity_id = request.data.get("opportunity")
        date_from = request.data.get("date_from")
        date_to = request.data.get("date_to")

        serializer = OpportunityTargetReportPayloadSerializer(data=request.data)

        if not serializer.is_valid():
            return Response(data=serializer.errors, status=HTTP_400_BAD_REQUEST)

        queryset = OpportunityTargetingReport.objects.filter(
            opportunity_id=opportunity_id,
            date_from=date_from,
            date_to=date_to,
            created_at__gte=self.get_expiration_datetime()
        )
        if not queryset.exists():
            report = serializer.save()
        else:
            report = queryset.first()

        user = request.user

        if not report.recipients.filter(pk=user.pk).exists():
            report.recipients.add(user)

        if report.status == ReportStatus.SUCCESS.value and report.s3_file_key:
            return Response(data=dict(
                message="Report is ready. Please download it by link below",
                download_link=OpportunityTargetingReportS3Exporter.generate_temporary_url(report.s3_file_key),
                status="ready",
            ))
        if report.status == ReportStatus.IN_PROGRESS.value:
            return Response(data=dict(
                message="Processing.  You will receive an email when your export is ready.",
                status="created",
            ))
        return Response(data=dict(
            message="Report failed.",
            status="failed",
        ))

    @staticmethod
    def get_expiration_datetime():
        return now_in_default_tz() - timedelta(hours=settings.REPORT_EXPIRATION_PERIOD)

    @staticmethod
    def get_visible_datetime():
        return now_in_default_tz() - timedelta(days=settings.REPORT_VISIBLE_PERIOD)
