from django.utils import timezone
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHistory


class PacingReportStatusApiView(APIView):
    """
    View for updating all Account and Campaign objects that have been synced with adwords
    """
    permission_classes = tuple()

    def patch(self, request, *_, **__):
        """
        Update Campaign sync time fields
            to indicate they have been synced with Adwords
        :param request.data: Campaign Ids to update
        """
        try:
            campaign_ids = request.data["campaignIds"]
            budget_history_ids = request.data["budgetHistoryIds"]
        except KeyError:
            return Response(status=HTTP_400_BAD_REQUEST, data="You must provide campaignIds and budgetHistoryIds "
                                                              "to update.")
        now = timezone.now()
        Campaign.objects.filter(id__in=campaign_ids).update(sync_time=now)
        CampaignHistory.objects.filter(id__in=budget_history_ids).update(sync_at=now)

        return Response(status=HTTP_200_OK,
                        data="Campaign budget sync complete for: {}".format(", ".join([str(c) for c in campaign_ids])))
