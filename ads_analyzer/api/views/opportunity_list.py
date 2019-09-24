from rest_framework.generics import ListAPIView

from ads_analyzer.api.serializers.opportunity_serializer import OpportunitySerializer
from aw_reporting.models import Opportunity


class OpportunityListAPIView(ListAPIView):
    queryset = Opportunity.objects
    serializer_class = OpportunitySerializer
