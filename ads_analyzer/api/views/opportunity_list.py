from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from ads_analyzer.api.serializers.opportunity_serializer import OpportunitySerializer
from aw_reporting.models import Opportunity
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class OpportunityListAPIView(ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_opportunity_list"),
            IsAdminUser,
        ),
    )
    queryset = Opportunity.objects.have_active_campaigns()
    serializer_class = OpportunitySerializer
