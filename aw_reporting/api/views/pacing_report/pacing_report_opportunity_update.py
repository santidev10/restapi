from django.contrib.auth import get_user_model
from rest_framework.generics import UpdateAPIView
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.serializers.pacing_report_opportunity_update_serializer import \
    PacingReportOpportunityUpdateSerializer
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from django.db.models import F
from django.db.models import Q
from django.db.models import Case
from django.db.models import When
from django.db.models import Value
from django.db.models import IntegerField


class PacingReportOpportunityUpdateApiView(UpdateAPIView):
    serializer_class = PacingReportOpportunityUpdateSerializer

    def get_queryset(self):
        return Opportunity.objects.get_queryset_for_user(user=self.request.user)

    def update(self, request, *args, **kwargs):
        response = super(PacingReportOpportunityUpdateApiView, self).update(
            request, *args, **kwargs)

        if response.status_code == HTTP_200_OK:
            cpm_buffer = request.data.get('cpm_buffer', 0)
            cpv_buffer = request.data.get('cpv_buffer', 0)

            self.update_opportunity_placements_buffers(cpm_buffer=cpm_buffer, cpv_buffer=cpv_buffer)

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

    def update_opportunity_placements_buffers(self, cpm_buffer: int = 0, cpv_buffer: int = 0) -> None:
        """
        Retrieves all OpPlacements for Opportunity and updates their cpm or cpv buffers if provided

        :param cpm_buffer: Integer
        :param cpv_buffer: Integer
        :return: None
        """
        try:
            cpm_buffer = int(cpm_buffer)
            cpv_buffer = int(cpv_buffer)
        except ValueError:
            raise ValueError('You must provide buffers that are integers / can be casted into integers.')

        opportunity = self.get_object()

        opportunity.cpv_buffer = cpv_buffer
        opportunity.cpm_buffer = cpm_buffer

        opportunity_serializer = PacingReportOpportunityUpdateSerializer(opportunity)
        opportunity_serializer.is_valid(raise_exception=True)

        return
        # TODO: see if is valid

        opportunity.save()

        OpPlacement\
            .objects\
            .filter(opportunity=opportunity) \
            .filter(Q(goal_type_id=0) | Q(goal_type_id=1)) \
            .annotate(cpm_buffer=Value(cpm_buffer, output_field=IntegerField())) \
            .annotate(cpv_buffer=Value(cpv_buffer, output_field=IntegerField())) \
            .update(
                goal_ordered_units=Case(
                    When(Q(goal_type=0 & ~Q(cpm_buffer=None)), then=F('ordered_units') + F('ordered_units') * F('cpm_buffer') / 100),
                    When(Q(goal_type=1 & ~Q(cpv_buffer=None)), then=F('ordered_units') + F('ordered_units') * F('cpv_buffer') / 100),
                    default=F('goal_ordered_units')
                )
            )
