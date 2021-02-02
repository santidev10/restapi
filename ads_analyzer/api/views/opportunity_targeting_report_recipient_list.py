from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework.generics import ListAPIView

from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportRecipientsSerializer
from ads_analyzer.models import OpportunityTargetingReport
from utils.datetime import now_in_default_tz
from userprofile.constants import StaticPermissions


class OpportunityTargetingReportRecipientsAPIView(ListAPIView):
    permission_classes = (
         StaticPermissions.has_perms(StaticPermissions.ADS_ANALYZER__RECIPIENTS),
    )
    serializer_class = OpportunityTargetReportRecipientsSerializer

    def get_queryset(self):
        reports = OpportunityTargetingReport.objects.filter(created_at__gte=self.get_visible_datetime())
        return get_user_model().objects.filter(opportunity_target_reports__in=reports).all().distinct()

    @staticmethod
    def get_visible_datetime():
        return now_in_default_tz() - timedelta(days=settings.REPORT_VISIBLE_PERIOD)
