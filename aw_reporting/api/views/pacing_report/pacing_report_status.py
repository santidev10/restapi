from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.models import Account

class PacingReportStatusApiView(APIView):
    """
    View for updating all Account and Campaign objects that have been synced with adwords
    """
    def patch(self, request, *_, **__):
        """
        Update Account hourly_updated_at fields
            to indicate they have been synced with Adwords

        :param request.data: (utc datetime) updated_at
        :param request.data: Account id to update hourly_updated_at field
        """
        try:
            account_id = request.data['account_id']
            hourly_updated_at = request.data['updated_at']
        except KeyError:
            return Response(status=HTTP_400_BAD_REQUEST, data='You must provide an account id and an update time.')

        Account.objects\
            .filter(id=account_id)\
            .update(hourly_updated_at=hourly_updated_at)

        return Response(status=HTTP_200_OK, data='Account update complete.')
