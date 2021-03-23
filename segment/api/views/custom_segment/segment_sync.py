from django.http import Http404
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from performiq.models import Account
from performiq.models import OAuthAccount
from segment.models import CustomSegment
from segment.models.constants import Params
from utils.views import get_object


class SegmentSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        """
        Get data from viewiq to update placements on GoogleAds
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        cid = request.query_params.get("cid")
        try:
            ctl = CustomSegment.objects.filter(gads_synced=False,
                                               params__contains={Params.GoogleAds.CID: cid}).first()
        except CustomSegment.DoesNotExist:
            raise Http404
        else:
            # Get export file from s3 and extract urls
            urls = ctl.s3.get_extract_export_ids(as_url=True)
            data = {
                "urls": urls,
                "ad_group_ids": ctl.params.get(Params.GoogleAds.AD_GROUP_IDS)
            }
            return Response(data)

    def patch(self, request, *args, **kwargs):
        # update synced status for ctl
        ctl = get_object(CustomSegment, id=kwargs.get("pk"))
        ctl.gads_synced = True
        ctl.update_sync_history(request.data["name"], Params.GoogleAds)
        ctl.save(update_fields=["gads_synced", "statistics"])
        return Response()

    def post(self, request, *args, **kwargs):
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
