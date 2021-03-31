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
        During GET request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        cid = int(kwargs.get("pk", -1))
        CustomSegment.objects.filter(gads_is_synced=False, params__gads_sync_data__cid=int(cid))
        params_filter = {f"params__{Params.GADS_SYNC_DATA}__cid": cid}
        ctl = get_object(CustomSegment, gads_is_synced=False, **params_filter)
        data = {"code": self._get_code(ctl)}
        return Response(data)

    def post(self, request, *args, **kwargs):
        """
        Update CTL with CID and Adgroup Ids to update Google Ads placements
        """
        data = request.data
        ctl = get_object(CustomSegment, id=kwargs["pk"])
        cid = get_object(Account, id=data.get(Params.CID))
        adgroup_ids = request.data.get(Params.ADGROUP_IDS)
        if not adgroup_ids or not isinstance(adgroup_ids, list):
            raise ValidationError("You must provide a list of AdGroup id's to update.")
        sync_data = {
            Params.CID: cid.id,
            Params.ADGROUP_IDS: adgroup_ids
        }
        ctl.gads_is_synced = False
        ctl.params[Params.GADS_SYNC_DATA] = sync_data
        ctl.save(update_fields=["params", "gads_is_synced"])
        return Response()

    def patch(self, request, *args, **kwargs):
        """
        Update Google Ads CTL sync status
        During PATCH request, the pk is actually an Account id (Google Ads CID) that is used to get
        the CTL. This is because the script that runs on Google Ads is unaware of ViewIQ data
        """
        ctl = get_object(CustomSegment, id=kwargs.get("pk"))
        account = get_object(Account, id=ctl.params.get(Params.CID))
        ctl.gads_is_synced = True
        ctl.update_sync_history(account.name, Results.GADS)
        ctl.save(update_fields=["gads_is_synced", "statistics"])
        return Response()

    def _get_code(self, ctl):
        script_fp = "segment/utils/create_placements.js"
        with open(script_fp, mode="r") as file:
            func = file.read()
        placement_type = SegmentTypeEnum(ctl.segment_type).name.capitalize()
        placement_ids = list(ctl.s3.get_extract_export_ids())
        ad_group_ids = ctl.params[Params.GADS_SYNC_DATA][Params.ADGROUP_IDS]
        # Replace placeholders in placement script with data to be evaluated and executed on Google Ads scripts
        code = func\
            .replace("{DOMAIN}", settings.HOST)\
            .replace("{adGroupIds}", json.dumps(ad_group_ids))\
            .replace("{placementIds}", json.dumps(placement_ids))\
            .replace("{placementBuilderType}", f"newYouTube{placement_type}Builder")\
            .replace("{placementIdType}", f"with{placement_type}Id") \
            .replace("{placementRemovalType}", f"youTube{placement_type}s") + "\nrun()"
        return code
