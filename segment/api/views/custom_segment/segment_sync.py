from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response

from segment.models import CustomSegment
from segment.models.constants import Params
from utils.views import get_object


class SegmentGoogleAdsSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        cid = request.query_params.get("cid")
        try:
            ctl = CustomSegment.objects.filter(gads_synced=False,
                                               params__contains={Params.GoogleAds.CID: cid}).first()
        except CustomSegment.DoesNotExist:
            raise Http404
        else:
            # Get export file from s3 and extract urls
            urls = ctl.s3.get_extract_export_ids(as_id=False)
            data = {
                "urls": urls,
                "ad_group_ids": ctl.params.get(Params.GoogleAds.AD_GROUP_IDS)
            }
            return Response(data)

    def patch(self, request, *args, **kwargs):
        # update synced status for ctl
        ctl = get_object(CustomSegment, id=kwargs.get("pk"))
        ctl.gads_synced = True
        ctl.save(update_fields=["gads_synced"])
        return Response()