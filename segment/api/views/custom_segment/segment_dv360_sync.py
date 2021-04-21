from django.http import Http404

from rest_framework.views import APIView
from rest_framework.response import Response

from audit_tool.models import AuditProcessor
from oauth.constants import OAuthType
from oauth.models import DV360Advertiser
from oauth.models import AdGroup
from segment.models.constants import Params
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from segment.models import CustomSegment
from utils.views import get_object


class SegmentDV360SyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update CTL with data to generate SDF
        """
        segment, advertiser, adgroup_ids = self._validate()
        params = {
            Params.ADGROUP_IDS: adgroup_ids,
            Params.ADVERTISER_ID: advertiser.id,
        }
        segment.update_params(params, Params.DV360_SYNC_DATA, save=True)
        self._start_audit(segment)
        response = {"message": f"Processing. You will receive an email when your DV360 SDF export for: {segment.title} "
                               f"is ready."}
        return Response(response)

    def _validate(self):
        data = self.request.data
        adgroup_ids = data.get("adgroup_ids", [])
        advertiser = get_object(DV360Advertiser, id=data.get("advertiser_id"))
        segment = get_object(CustomSegment, id=self.kwargs.get("pk"))
        exists = AdGroup.objects.filter(id__in=adgroup_ids, oauth_type=int(OAuthType.DV360))
        remains = set(adgroup_ids) - set(exists.values_list("id", flat=True))
        if not exists or remains:
            raise Http404(f"Unknown Adgroup ids: {remains}")
        return segment, advertiser, adgroup_ids

    def _start_audit(self, segment):
        audit_type = segment.config.AUDIT_TYPE
        audit_params = {
            "seed_file": "",
            "do_videos": 0,
            "force_data_refresh": 1,
            "segment_id": segment.id,
            "audit_type_original": audit_type,
            "user_id": self.request.user.id,
            "start_dv360_task": True,
        }
        audit = AuditProcessor.objects.create(source=2, audit_type=audit_type, name=segment.title.lower(), params=audit_params, temp_stop=True)
        GenerateSegmentUtils(segment).start_audit(segment.get_s3_key(), audit)
