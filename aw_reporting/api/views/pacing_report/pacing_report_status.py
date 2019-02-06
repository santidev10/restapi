from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from utils.datetime import Time

class PacingReportStatusApiView(APIView):
    """
    View for updating all Account and Campaign objects that have been synced with adwords
    """
    @property
    def now(self):
        return Time().now()

    def patch(self, request, *_, **__):
        """
        Update Account and Campaigns hourly_updated_at fields
            to indicate they have been synced with Adwords

        :param request: request -> (utc datetime) updated_at
        :param account_ids: Account ids to update hourly_updated_at
        :param campaign_ids: Campaigns ids to update hourly_updated_at
        """
        account_ids = request.data.get('account_ids', [])
        campaign_ids = request.data.get('campaign_ids', [])
        updated_at = request.data.get('updated_at', self.now)

        # update all accounts
        Account.objects\
            .filter(id__in=account_ids)\
            .update(hourly_updated_at=updated_at)

        Campaign.objects\
            .filter(id__in=campaign_ids)\
            .update(updated_time=updated_at)

        return Response(status=HTTP_200_OK, data='Campaigns and Accounts status update complete.')
