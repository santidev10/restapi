from rest_framework.views import APIView
from rest_framework.response import Response

from segment.tasks.generate_sdf import generate_sdf
from utils.views import get_object


class SegmentSyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update SegmentAdGroupSync record with CID and Adgroup Ids to update Google Ads placements
        """
        data = request.data
        advertiser_id = kwargs["pk"]
        segment_id = data["segment_id"]
        io_ids = data["insertion_order_ids"]
        generate_sdf.delay(segment_id, advertiser_id, io_ids)
        return Response()
