from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response

from oauth.models import DV360Advertiser
from oauth.models import InsertionOrder
from segment.models import CustomSegment
from segment.tasks.generate_sdf import generate_sdf_task
from utils.views import get_object


class SegmentDV360SyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update SegmentAdGroupSync record with CID and Adgroup Ids to update Google Ads placements
        """
        segment_id, advertiser_id, line_item_ids = self._validate()
        generate_sdf_task.delay(request.user.email, segment_id, advertiser_id, line_item_ids)
        return Response()

    def _validate(self):
        data = self.request.data
        line_item_ids = data.get("line_item_ids", [])
        advertiser = get_object(DV360Advertiser, id=self.kwargs.get("pk"))
        segment = get_object(CustomSegment, id=data.get("segment_id"))
        exists = InsertionOrder.objects.filter(id__in=line_item_ids)
        remains = set(line_item_ids) - set(exists.values_list("id", flat=True))
        if remains:
            raise ValidationError(f"Unknown LineItem ids: {remains}")
        return segment.id, advertiser.id, line_item_ids
