from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from segment.models import CustomSegment
from segment.models.constants import Params
from oauth.models import Account
from oauth.models import OAuthAccount
from utils.views import get_object


class SegmentOAuthAPIView(APIView):
    def get(self, request, *args, **kwargs):
        # get oauth cid or adgroup ids
        oauth_account = get_object(OAuthAccount, user=request.user, message="OAuth account not found. Please OAuth.")
        cid = request.query_params.get("cid")
        if cid:
            qs = AdGroup.objects.filter(campaign__account=cid)
        else:
            qs = oauth_account.gads_accounts.all()
        data = qs.values_list("id", flat=True)
        return Response(dict(data=data))

    def patch(self, request, *args, **kwargs):
        """ Update ctl for gads sync """
        data = request.data
        ctl = get_object(CustomSegment, id=kwargs["pk"])
        cid = get_object(Account, id=data.get(Params.GoogleAds.CID))
        ad_group_ids = request.data.get(Params.GoogleAds.AD_GROUP_IDS)
        if not ad_group_ids or not isinstance(ad_group_ids, list):
            raise ValidationError("You must provide a list of AdGroup id's to update.")
        sync_data = {
            Params.GoogleAds.CID: cid.id,
            Params.GoogleAds.AD_GROUP_IDS: ad_group_ids
        }
        ctl.params[Params.GoogleAds.GADS_SYNC_DATA] = sync_data
        ctl.save(update_fields=["params"])
        return Response()
