from botocore.exceptions import ClientError
from rest_framework.exceptions import NotFound
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from uuid import uuid4

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
        DV360 SDF uploads are validated for existing Youtube Channels and Videos. We must filter the CustomSegment
        export result for existing Youtube resources and is done with the audit tool app
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
            raise NotFound(f"Unknown Adgroup ids: {remains}")
        return segment, advertiser, adgroup_ids

    def _start_audit(self, segment):
        try:
            segment_export_fp = self._download_segment_export(segment)
        except ClientError:
            raise NotFound(f"Export not found for segment: {segment.title}")
        audit_type = segment.config.AUDIT_TYPE
        # Seed file will be updated with GenerateSegmentUtils.start_audit
        audit_params = {
            "seed_file": "",
            "do_videos": 0,
            "num_videos": 0,
            "force_data_refresh": 1,
            "name": segment.title,
            "segment_id": segment.id,
            "audit_type_original": audit_type,
            "user_id": self.request.user.id,
            "with_dv360_sdf": True,
        }
        # Audit.temp_stop will be False once seed file is uploaded with start_audit method
        audit = AuditProcessor.objects.create(source=2, audit_type=audit_type, name=segment.title.lower(),
                                              params=audit_params, temp_stop=True)
        GenerateSegmentUtils(segment).start_audit(segment_export_fp, audit)

    def _download_segment_export(self, segment):
        """ Download export to create audit seed file """
        fp = f"{settings.TEMPDIR}/{uuid4()}.csv"
        segment.s3.download_file(segment.get_s3_key(), fp)
        return fp
