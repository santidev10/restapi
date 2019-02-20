from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import Account
from utils.datetime import Time
from pytz import utc

class PacingReportStatusApiView(APIView):
    """
    View for updating all Account and Campaign objects that have been synced with adwords
    """
    @property
    def now(self):
        return Time().now(tz=utc)

    def patch(self, request, *_, **__):
        """
        Update Account and Campaigns hourly_updated_at fields
            to indicate they have been synced with Adwords

        :param request: request -> (utc datetime) updated_at
        :param account_ids: Account ids to update hourly_updated_at
        :param campaign_ids: Campaigns ids to update hourly_updated_at
        """
        account_ids = request.data.get('account_ids', [])
        hourly_updated_at = request.data.get('hourly_updated_at', self.now)

        # update all accounts
        Account.objects\
            .filter(id__in=account_ids)\
            .update(hourly_updated_at=hourly_updated_at)

        return Response(status=HTTP_200_OK, data='Campaigns and Accounts status update complete.')
