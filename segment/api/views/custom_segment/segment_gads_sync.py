from rest_framework.views import APIView
from rest_framework.exceptions import PermissionDenied
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import OAuthAccount
from segment.models.models import SegmentAdGroupSync
from segment.api.serializers.gads_sync_serializer import GadsSyncSerializer
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from segment.utils.utils import get_gads_sync_code
from utils.views import get_object


class SegmentSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        """
        Get data from ViewIQ to update placements on GoogleAds
        During GET request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        account = self._get_account()
        code = get_gads_sync_code(account)
        res = {
            "code": code,
        }
        return Response(res)

    def post(self, request, *args, **kwargs):
        """
        Update SegmentAdGroupSync record with CID and Adgroup Ids to update Google Ads placements
        """
        data = request.data
        account = self._get_account()
        gads_oauth = get_object(OAuthAccount, user=request.user, oauth_type=OAuthType.GOOGLE_ADS.value, is_enabled=True)
        # Check that user has access to cid
        try:
            account.oauth_accounts.get(id=gads_oauth.id)
        except OAuthAccount.DoesNotExist:
            raise PermissionDenied(f"You do not have access for this account: {account.name}")
        serializer = GadsSyncSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response()

    def patch(self, request, *args, **kwargs):
        """
        Update Google Ads CTL sync status
        During PATCH request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        account = self._get_account()
        adgroup_ids = request.data["adgroup_ids"]
        ag_syncs = SegmentAdGroupSync.objects.filter(adgroup_id__in=adgroup_ids).select_related("segment")
        ag_syncs.update(is_synced=True)
        seen = set()
        for sync in ag_syncs:
            segment = sync.segment
            if sync.segment.id not in seen:
                segment.update_sync_history(account.name, Results.GADS_SYNC_DATA)
                segment.save(update_fields=["statistics"])
            seen.add(segment.id)
        return Response()
    
    def _get_account(self):
        account = get_object(Account, id=int(self.kwargs.get("pk", -1)))
        return account
