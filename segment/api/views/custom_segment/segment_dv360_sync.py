from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response

from oauth.models import DV360Advertiser
from oauth.models import InsertionOrder
from segment.models import CustomSegment
from segment.tasks.generate_sdf import generate_sdf
from utils.views import get_object


class SegmentDV360SyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update SegmentAdGroupSync record with CID and Adgroup Ids to update Google Ads placements
        """
        segment_id, advertiser_id, io_ids = self._validate()
        generate_sdf.delay(request.user.email, segment_id, advertiser_id, io_ids)
        return Response()

    def _validate(self):
        data = self.request.data
        io_ids = data.get("insertion_order_ids", [])
        advertiser = get_object(DV360Advertiser, id=self.kwargs.get("pk"))
        segment = get_object(CustomSegment, id=data.get("segment_id"))
        exists = InsertionOrder.objects.filter(id__in=io_ids)
        remains = set(io_ids) - set(exists.values_list("id", flat=True))
        if remains:
            raise ValidationError(f"Unknown insertion order ids: {remains}")
        return segment.id, advertiser.id, io_ids
