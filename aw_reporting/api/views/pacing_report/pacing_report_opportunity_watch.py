""" View to handle saving user watched SF Opportunities """
from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import Opportunity
from dashboard.models import OpportunityWatch
from utils.views import get_object


class PacingReportOpportunityWatchAPIView(APIView):
    MAX_WATCH = 5

    def patch(self, request, *args, **kwargs):
        opportunity = get_object(Opportunity, id=kwargs["pk"])
        user = request.user
        if OpportunityWatch.objects.filter(user=user).count() >= self.MAX_WATCH:
            raise ValidationError(f"You may only watch a max of {self.MAX_WATCH} opportunities.")
        else:
            OpportunityWatch.objects.get_or_create(user=user, opportunity=opportunity)
        return Response(status=HTTP_200_OK)

    def delete(self, request, *args, **kwargs):
        opportunity = get_object(Opportunity, id=kwargs["pk"])
        user = request.user
        OpportunityWatch.objects.filter(user=user, opportunity=opportunity).delete()
        return Response(status=HTTP_200_OK)
