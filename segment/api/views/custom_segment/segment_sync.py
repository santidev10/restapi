from django.http import Http404
from rest_framework.views import APIView

from segment.models import CustomSegment
from segment.models.constants import Params


class CTLGoogleAdsSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        cid = request.query_params.get("cid")
        try:
            ctl = CustomSegment.objects.filter(gads_synced=False,
                                               params__contains={Params.GoogleAds.CID: cid}).first()
        except CustomSegment.DoesNotExist:
            raise Http404
        else:
            # Get export file from s3 and extract urls
            pass

    def patch(self, request, *args, **kwargs):
        """ Update ctl for gads sync """
