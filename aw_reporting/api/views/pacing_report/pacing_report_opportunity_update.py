from django.contrib.auth import get_user_model
from rest_framework.generics import UpdateAPIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response

from aw_reporting.api.serializers.pacing_report_opportunity_update_serializer import \
    PacingReportOpportunityUpdateSerializer
from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from .pacing_report_helper import PacingReportHelper


class PacingReportOpportunityUpdateApiView(UpdateAPIView):
    serializer_class = PacingReportOpportunityUpdateSerializer

    def get_queryset(self):
        return Opportunity.objects.get_queryset_for_user(user=self.request.user)

    def update(self, request, *args, **kwargs):
        response = super(PacingReportOpportunityUpdateApiView, self).update(
            request, *args, **kwargs)

        if response.status_code == HTTP_200_OK:
            response.data['thumbnail'] = None
            ad_ops = self.get_object().ad_ops_manager
            if ad_ops:
                profile_images = get_user_model().objects.filter(
                    email=ad_ops.email,
                    profile_image_url__isnull=False,
                ).exclude(profile_image_url="").values_list(
                    "profile_image_url", flat=True)
                if profile_images:
                    response.data['thumbnail'] = profile_images[0]
        return response


class PacingReportOpportunityBufferUpdateApiView(UpdateAPIView):
    serializer_class = PacingReportOpportunityUpdateSerializer
    permission_classes = ()

    def update(self, request, pk, **_):
        try:
            opportunity = Opportunity.objects.get(pk=pk)
        except Opportunity.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        allowed_updates = ['cpm_buffer', 'cpv_buffer']

        if not set(request.data.keys()).issubset(allowed_updates):
            return Response(status=HTTP_400_BAD_REQUEST, data='You may only update cpm_buffer or cpv_buffer values.')

        Opportunity.objects.filter(pk=pk).update(**request.data)

        # Get placement for opportunity
        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.multiply_percents(placements)

        return Response(status=HTTP_200_OK, data=placements)
