from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportModelSerializer
from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportPayloadSerializer
from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.create_opportunity_targeting_report import OpportunityTargetingReportS3Exporter
from utils.api_paginator import CustomPageNumberPaginator
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class Paginator(CustomPageNumberPaginator):
    page_size = 10


class OpportunityTargetingReportAPIView(ListCreateAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.create_opportunity_report"),
            IsAdminUser,
        ),
    )
    queryset = OpportunityTargetingReport.objects.all()
    serializer_class = OpportunityTargetReportModelSerializer
    pagination_class = Paginator

    def post(self, request, *args, **kwargs):
        serializer = OpportunityTargetReportPayloadSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(data=serializer.errors, status=HTTP_400_BAD_REQUEST)
        report = serializer.save()
        user = request.user
        if not report.recipients.filter(pk=user.pk).exists():
            report.recipients.add(user)

        if report.status == ReportStatus.SUCCESS.value and report.s3_file_key:
            return Response(data=dict(
                message="Report is ready. Please download it by link below",
                report_link=OpportunityTargetingReportS3Exporter.generate_temporary_url(report.s3_file_key),
                status="ready",
            ))
        elif report.status == ReportStatus.IN_PROGRESS.value:
            return Response(data=dict(
                message="Processing.  You will receive an email when your export is ready.",
                status="created",
            ))
        return Response(data=dict(
            message="Report failed.",
            status="failed",
        ))
