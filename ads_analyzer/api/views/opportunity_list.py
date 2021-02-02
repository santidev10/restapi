from datetime import timedelta

from django.utils import timezone
from rest_framework.generics import ListAPIView

from ads_analyzer.api.serializers.opportunity_serializer import OpportunitySerializer
from aw_reporting.models import Opportunity
from userprofile.constants import StaticPermissions


class OpportunityListAPIView(ListAPIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.ADS_ANALYZER),
    )
    queryset = Opportunity.objects.filter(start__gte=timezone.now() - timedelta(days=365),
                                          start__lte=timezone.now()).values("id", "name", "start")
    serializer_class = OpportunitySerializer
