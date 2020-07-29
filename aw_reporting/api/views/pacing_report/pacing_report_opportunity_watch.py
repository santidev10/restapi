""" View to handle saving user watched SF Opportunities """
from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_204_NO_CONTENT

from aw_reporting.models import Opportunity
from utils.views import get_object


class PacingReportOpportunityWatchAPIView(APIView):
    MAX_WATCH = 5

    def patch(self, request, *args, **kwargs):
        opportunity = get_object(Opportunity, id=kwargs["pk"])
        user = request.user
        if user.opportunities.count() >= self.MAX_WATCH:
            raise ValidationError(f"You may only watch a max of {self.MAX_WATCH} opportunities.")
        else:
            user.opportunities.add(opportunity)
        return Response(data=HTTP_204_NO_CONTENT)
