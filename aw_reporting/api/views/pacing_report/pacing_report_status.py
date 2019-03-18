from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from django.utils import timezone
from aw_reporting.models import Campaign

class PacingReportStatusApiView(APIView):
    """
    View for updating all Account and Campaign objects that have been synced with adwords
    """
    permission_classes = tuple()

    def patch(self, request, *_, **__):
        """
        Update Camapign sync time fields
            to indicate they have been synced with Adwords

        :param request.data: Campaign Ids to update
        """
        try:
            campaign_ids = request.data['campaignIds']

        except KeyError:
            return Response(status=HTTP_400_BAD_REQUEST, data='You must provide campaignIds to update.')

        Campaign.objects.filter(id__in=campaign_ids).update(sync_time=timezone.now())

        return Response(status=HTTP_200_OK, data='Campaigns sync complete.')
