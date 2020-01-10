from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model

from rest_framework.generics import ListAPIView

from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportRecipientsSerializer
from ads_analyzer.models import OpportunityTargetingReport
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class OpportunityTargetingReportRecipientsAPIView(ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_opportunity_report_recipients_list"),
        ),
    )

    serializer_class = OpportunityTargetReportRecipientsSerializer

    def get_queryset(self):
        reports = OpportunityTargetingReport.objects.filter(created_at__gte=self.get_expiration_datetime())
        return get_user_model().objects.filter(opportunity_target_reports__in=reports).all().distinct()

    @staticmethod
    def get_expiration_datetime():
        return now_in_default_tz() - timedelta(hours=settings.REPORT_EXPIRATION_PERIOD)
