from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response

from oauth.constants import OAuthType
from oauth.models import DV360Advertiser
from oauth.models import AdGroup
from segment.models.constants import Params
from segment.models import CustomSegment
from utils.views import get_object


class SegmentDV360SyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update CTL with data to generate SDF
        """
        segment_id, advertiser_id, adgroup_ids = self._validate()
        segment = CustomSegment.objects.get(id=segment_id)
        params = {
            Params.ADGROUP_IDS: adgroup_ids,
            Params.ADVERTISER_ID: advertiser_id,
        }
        segment.update_params(params, Params.DV360_SYNC_DATA, save=True)
        return Response()

    def _validate(self):
        data = self.request.data
        adgroup_ids = data.get("adgroup_ids", [])
        advertiser = get_object(DV360Advertiser, id=self.kwargs.get("pk"))
        segment = get_object(CustomSegment, id=data.get("segment_id"))
        exists = AdGroup.objects.filter(id__in=adgroup_ids, oauth_type=int(OAuthType.DV360))
        remains = set(adgroup_ids) - set(exists.values_list("id", flat=True))
        if not exists or remains:
            raise ValidationError(f"Unknown Adgroup ids: {remains}")
        return segment.id, advertiser.id, adgroup_ids
