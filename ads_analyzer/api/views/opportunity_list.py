from rest_framework.generics import ListAPIView
from ads_analyzer.api.serializers.opportunity_serializer import OpportunitySerializer
from aw_reporting.models import Opportunity
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from django.utils import timezone
from datetime import timedelta

class OpportunityListAPIView(ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_opportunity_list"),
        ),
    )
    queryset = Opportunity.objects.filter(start__gte=timezone.now()-timedelta(days=365)\
        ).values("id", "name")
    serializer_class = OpportunitySerializer