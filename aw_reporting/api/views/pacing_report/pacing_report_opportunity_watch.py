""" View to handle saving user watched SF Opportunities """
from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from .constants import PACING_REPORT_OPPORTUNITIES_MAX_WATCH
from aw_reporting.models import Opportunity
from cache.models import CacheItem
from dashboard.api.views import DashboardPacingAlertsAPIView
from dashboard.models import OpportunityWatch
from utils.views import get_object


class PacingReportOpportunityWatchAPIView(APIView):

    def patch(self, request, *args, **kwargs):
        opportunity = get_object(Opportunity, id=kwargs["pk"])
        user = request.user
        if OpportunityWatch.objects.filter(user=user).count() >= PACING_REPORT_OPPORTUNITIES_MAX_WATCH:
            raise ValidationError(f"You may only watch a max of {PACING_REPORT_OPPORTUNITIES_MAX_WATCH} opportunities.")
        else:
            OpportunityWatch.objects.get_or_create(user=user, opportunity=opportunity)
        self._invalidate_cache(request.user.id)
        return Response(status=HTTP_200_OK)

    def delete(self, request, *args, **kwargs):
        opportunity = get_object(Opportunity, id=kwargs["pk"])
        user = request.user
        OpportunityWatch.objects.filter(user=user, opportunity=opportunity).delete()
        self._invalidate_cache(request.user.id)
        return Response(status=HTTP_200_OK)

    def _invalidate_cache(self, user_id):
        pacing_alerts_cache_key = DashboardPacingAlertsAPIView.get_cache_key(user_id)
        CacheItem.objects.filter(key=pacing_alerts_cache_key).delete()
