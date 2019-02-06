from django.contrib.auth import get_user_model
from rest_framework.generics import UpdateAPIView
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.serializers.pacing_report_opportunity_update_serializer import \
    PacingReportOpportunityUpdateSerializer
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement


class PacingReportOpportunityUpdateApiView(UpdateAPIView):
    serializer_class = PacingReportOpportunityUpdateSerializer

    def get_queryset(self):
        return Opportunity.objects.get_queryset_for_user(user=self.request.user)

    def update(self, request, *args, **kwargs):
        response = super(PacingReportOpportunityUpdateApiView, self).update(
            request, *args, **kwargs)

        cpm_buffer = request.data.get('cpm_buffer', None)
        cpv_buffer = request.data.get('cpv_buffer', None)

        self.update_opportunity_placements_buffers(cpm_buffer=cpm_buffer, cpv_buffer=cpv_buffer)

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

    def update_opportunity_placements_buffers(self, cpm_buffer=None, cpv_buffer=None):
        """
        Retrieves all OpPlacements for Opportunity and updates their cpm or cpv buffers if provided

        :param cpm_buffer: Integer
        :param cpv_buffer: Integer
        :return: None
        """
        opportunity = self.get_object()

        for placement in OpPlacement.objects.filter(opportunity=opportunity):
            if cpm_buffer is not None and placement.goal_type == 'CPM':
                new_cpm_ordered_units_goal = placement.ordered_units + (placement.ordered_units * cpm_buffer / 100)
                new_cpm_ordered_units_goal = new_cpm_ordered_units_goal if new_cpm_ordered_units_goal > 0 else 0

                placement.goal_ordered_units = new_cpm_ordered_units_goal
                placement.save()

            if cpv_buffer is not None and placement.goal_type == 'CPM':
                new_cpv_ordered_units_goal = placement.ordered_units + (placement.ordered_units * cpv_buffer / 100)
                new_cpv_ordered_units_goal = new_cpv_ordered_units_goal if new_cpv_ordered_units_goal > 0 else 0
                
                placement.goal_ordered_units = new_cpv_ordered_units_goal
                placement.save()