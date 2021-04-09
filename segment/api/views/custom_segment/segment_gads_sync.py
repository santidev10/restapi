from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import F
from rest_framework.permissions import BasePermission
from rest_framework.views import APIView
from rest_framework.exceptions import PermissionDenied
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.response import Response

from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import OAuthAccount
from segment.models import CustomSegment
from segment.models import SegmentAdGroupSync
from segment.api.serializers.gads_sync_serializer import GadsSyncSerializer
from segment.models.constants import Results
from segment.utils.utils import get_gads_sync_code
from utils.views import get_object


class OAuthAPITokenPermissionClass(BasePermission):
    """ Check if oauth api key is valid """
    def has_permission(self, request, view):
        if request.method.lower() in {"get", "patch"}:
            try:
                viq_key = request.query_params.get("viq_key")
                has_permission = get_object(OAuthAccount, viq_key=viq_key, should_raise=False)
            # If viq_key is invalid UUID
            except DjangoValidationError:
                has_permission = False
        else:
            has_permission = request.user and request.user.is_authenticated
        return has_permission


class SegmentGadsSyncAPIView(APIView):
    permission_classes = (OAuthAPITokenPermissionClass,)

    def get(self, request, *args, **kwargs):
        """
        Get data from ViewIQ to update placements on GoogleAds
        """
        account = self._get_account()
        query_params = request.query_params
        if query_params.get("as_mcc"):
            viq_key = query_params["viq_key"]
            oauth_account = get_object(OAuthAccount, viq_key=viq_key)
            # If running as an MCC, the Google Ads scripts requires the cid account ids to individually request the
            # placement creation code for each account
            cid_ids = SegmentAdGroupSync.objects\
                .filter(adgroup__campaign__account__oauth_accounts=oauth_account)\
                .annotate(cid=F("adgroup__campaign__account_id"))\
                .distinct()\
                .values_list("cid", flat=True)
            res = cid_ids
        else:
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
        Update Google Ads CTL sync status and add sync history
        """
        account = self._get_account()
        adgroup_ids = request.data["adgroup_ids"]
        ag_syncs = SegmentAdGroupSync.objects.filter(adgroup_id__in=adgroup_ids).select_related("segment")
        ag_syncs.update(is_synced=True)
        seen = set()
        ctl_to_update = []
        for sync in ag_syncs:
            segment = sync.segment
            if sync.segment.id not in seen:
                segment.update_sync_history(account.name, Results.GADS_SYNC_DATA)
                ctl_to_update.append(segment)
            seen.add(segment.id)
        CustomSegment.objects.bulk_update(ctl_to_update, fields=["statistics"])
        return Response()
    
    def _get_account(self):
        account = get_object(Account, id=int(self.kwargs.get("pk", -1)))
        return account
