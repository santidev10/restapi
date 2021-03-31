import json

from django.conf import settings
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from oauth.models import Account
from segment.models.constants import SegmentTypeEnum
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from utils.views import get_object


class SegmentSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        """
        Get data from ViewIQ to update placements on GoogleAds
        """
        cid = kwargs.get("pk")
        # ctl = get_object(CustomSegment, gads_is_synced=False, params__contains={Params.GoogleAds.CID: cid})
        ctl = get_object(CustomSegment, gads_is_synced=False, params__contains={Params.GoogleAds.CID: cid})
        data = {"code": self._get_code(ctl)}
        return Response(data)

    def post(self, request, *args, **kwargs):
        """
        Update CTL with CID and Adgroup Ids to update Google Ads placements
        """
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

    def patch(self, request, *args, **kwargs):
        """
        Update Google Ads CTL sync status
        """
        ctl = get_object(CustomSegment, id=kwargs.get("pk"))
        account = get_object(Account, id=ctl.params.get(Params.GoogleAds.CID))
        ctl.gads_is_synced = True
        ctl.update_sync_history(account.name, Results.GADS)
        ctl.save(update_fields=["gads_is_synced", "statistics"])
        return Response()

    def _get_code(self, ctl):
        script_fp = "segment/utils/create_placements.js"
        with open(script_fp, mode="r") as file:
            func = file.read()
        placement_type = SegmentTypeEnum(ctl.segment_type).name.capitalize()
        placement_ids = ctl.s3.get_extract_export_ids()
        ad_group_ids = ctl.params[Params.GoogleAds.AD_GROUP_IDS]
        code = func\
            .replace("{DOMAIN}", settings.HOST)\
            .replace("{adGroupIds}", json.dumps(ad_group_ids))\
            .replace("{placementIds}", json.dumps(placement_ids))\
            .replace("{placementBuilderType}", f"newYouTube{placement_type}Builder")\
            .replace("{placementIdType}", f"with{placement_type}Id") \
            .replace("{placementRemovalType}", f"youTube{placement_type}s")
        return code
