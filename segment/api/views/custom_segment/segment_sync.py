import json

from django.conf import settings
from rest_framework.views import APIView
from rest_framework.exceptions import PermissionDenied
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import OAuthAccount
from segment.models.models import SegmentSync
from segment.models.constants import SegmentTypeEnum
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from utils.views import get_object


class SegmentSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        """
        Get data from ViewIQ to update placements on GoogleAds
        During GET request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        account = self._get_account()
        segment = account.sync.segment
        # Only provide update code if is_synced is False
        if hasattr(account, "sync") and account.sync.is_synced is False:
            code = self._get_code(segment)
        else:
            code = None
        data = {"code": code}
        return Response(data)

    def post(self, request, *args, **kwargs):
        """
        Update SegmentSync record with CID and Adgroup Ids to update Google Ads placements
        """
        data = request.data
        account = self._get_account()
        gads_oauth = get_object(OAuthAccount, user=request.user, oauth_type=OAuthType.GOOGLE_ADS.value, is_enabled=True)
        # Check that user has access to cid
        try:
            account.oauth_accounts.get(id=gads_oauth.id)
        except OAuthAccount.DoesNotExist:
            raise PermissionDenied(f"You do not have access for this account: {account.name}")
        ctl_id = data.get(Params.SEGMENT_ID)
        adgroup_ids = data.get(Params.ADGROUP_IDS)
        segment = get_object(CustomSegment, id=ctl_id)
        if not adgroup_ids or not isinstance(adgroup_ids, list):
            raise ValidationError("You must provide a list of AdGroup id's to update.")
        sync_data = {Params.ADGROUP_IDS: adgroup_ids}
        SegmentSync.objects.update_or_create(account=account, defaults=dict(segment=segment, data=sync_data, is_synced=False))
        return Response()

    def patch(self, request, *args, **kwargs):
        """
        Update Google Ads CTL sync status
        During PATCH request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        account = self._get_account()
        sync = account.sync
        segment = sync.segment
        # segment = get_object(CustomSegment, id=kwargs.get("pk"))
        # account = get_object(Account, id=segment.params.get(Params.GADS_SYNC_DATA, {}).get(Params.CID))
        segment.update_sync_history(account.name, Results.GADS_SYNC_DATA)
        segment.save(update_fields=["statistics"])
        sync.is_synced = True
        sync.save(update_fields=["is_synced"])
        return Response()

    def _get_code(self, segment):
        script_fp = "segment/utils/create_placements.js"
        with open(script_fp, mode="r") as file:
            func = file.read()
        placement_type = SegmentTypeEnum(segment.segment_type).name.capitalize()
        placement_ids = list(segment.s3.get_extract_export_ids())
        ad_group_ids = segment.params[Params.GADS_SYNC_DATA][Params.ADGROUP_IDS]
        # Replace placeholders in placement script with data to be evaluated and executed on Google Ads scripts
        code = func\
            .replace("{DOMAIN}", settings.HOST)\
            .replace("{adGroupIds}", json.dumps(ad_group_ids))\
            .replace("{placementIds}", json.dumps(placement_ids))\
            .replace("{placementBuilderType}", f"newYouTube{placement_type}Builder")\
            .replace("{placementIdType}", f"with{placement_type}Id") \
            .replace("{placementRemovalType}", f"youTube{placement_type}s") + "\nrun()"
        return code
    
    def _get_account(self):
        account = get_object(Account, id=int(self.kwargs.get("pk", -1)))
        return account
