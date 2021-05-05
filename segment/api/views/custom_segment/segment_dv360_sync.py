from datetime import timedelta

from botocore.exceptions import ClientError
from django.conf import settings
from django.db.models import F
from django.db.models.functions import Coalesce
from rest_framework.exceptions import NotFound
from rest_framework.response import Response
from rest_framework.views import APIView
from uuid import uuid4

from audit_tool.models import AuditProcessor
from oauth.constants import OAuthType
from oauth.models import DV360Advertiser
from oauth.models import AdGroup
from segment.models.constants import Params
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from segment.models import CustomSegment
from segment.tasks.generate_sdf_segment import generate_sdf_segment_task
from utils.views import get_object
from utils.datetime import now_in_default_tz


class SegmentDV360SyncAPIView(APIView):

    def post(self, request, *args, **kwargs):
        """
        Update CustomSegment and create AuditProcessor with request data to generate SDF
        DV360 SDF uploads are validated for existing Youtube Channels and Videos. We must filter the CustomSegment
        export for existing Youtube resources through audit queue for SDF uploads to work

        Flow:
            1. _validate -> Validate client post params
            2. _process -> Check if a valid AuditProcessor exists for the target CustomSegment and determine task.
                Audit validity is checked in _get_audit
            3.
                If no valid audit exists: _start_audit -> Begin audit flow in audit_tool app. This will trigger
                    an audit for the CustomSegment and will call generate_sdf_task in audit_video_meta.py
                If valid audit exists: generate_sdf_task -> Generate SDF export directly with valid AuditProcessor data
        """
        segment = self._process(*self._validate())
        response = {"message": f"Processing. You will receive an email when your DV360 SDF export for: {segment.title} "
                               f"is ready."}
        return Response(response)

    def _process(self, segment: CustomSegment, advertiser: DV360Advertiser, adgroup_ids: list[int]) -> CustomSegment:
        """
        Determine if new audit must be created or generate_sdf_task can be called immediately
        :param segment: CustomSegment retrieved from request pk
        :param advertiser: Parent DV360Advertiser of adgroup_ids
        :param adgroup_ids: ids of DV360 Adgroups to update placements for
        :return: CustomSegment
        """
        # If a valid audit exists for this segment, then immediately start generate_sdf_task, else start a new one
        audit = self._get_audit(segment)
        if audit is None:
            created_audit = self._start_audit(segment, advertiser.id, adgroup_ids)
            # Save audit id on CustomSegment to later check if we can use same audit to generate SDF in this view
            segment.update_params(created_audit.id, Params.DV360_SYNC_DATA, data_field=Params.META_AUDIT_ID, save=True)
        else:
            generate_sdf_segment_task.delay(self.request.user.id, audit.id, segment.id, advertiser.id, adgroup_ids)
        return segment

    def _validate(self) -> tuple:
        """
        Validate post request url and query params
        All resources must exist to pass validation
        """
        data = self.request.data
        adgroup_ids = data.get("adgroup_ids", [])
        segment = get_object(CustomSegment, id=self.kwargs.get("pk"))
        exists = AdGroup.objects.filter(id__in=adgroup_ids, oauth_type=int(OAuthType.DV360))\
            .annotate(advertiser_id=F("line_item__insertion_order__campaign__advertiser_id"))
        remains = set(adgroup_ids) - set(exists.values_list("id", flat=True))
        if not exists or remains:
            raise NotFound(f"Unknown Adgroup ids: {remains}")
        advertiser = None
        for ag in exists:
            advertiser = get_object(DV360Advertiser, id=ag.advertiser_id, should_raise=False)
            if advertiser:
                break
        if advertiser is None:
            raise NotFound(f"Unable to find advertiser for Adgroup ids: {adgroup_ids}")
        return segment, advertiser, adgroup_ids

    def _start_audit(self, segment, advertiser_id, adgroup_ids) -> AuditProcessor:
        """
        Create audit to filter for valid placements in CTL export. DV360 SDF uploads will break if a placement
            is invalid e.g. Removed from YouTube
        :param segment: CustomSegment
        :return: Created AuditProcessor
        """
        try:
            # Get export to generate audit seed file
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
            Params.DV360_SYNC_DATA: {
                Params.ADGROUP_IDS: adgroup_ids,
                Params.ADVERTISER_ID: advertiser_id,
            }
        }
        # Audit.temp_stop will be False once seed file is uploaded with start_audit method
        audit = AuditProcessor.objects.create(source=2, audit_type=audit_type, name=segment.title.lower(),
                                              params=audit_params, temp_stop=True)
        GenerateSegmentUtils(segment).start_audit(segment_export_fp, audit)
        return audit

    def _download_segment_export(self, segment: CustomSegment):
        """ Download export to create audit seed file """
        fp = f"{settings.TEMPDIR}/{uuid4()}.csv"
        segment.s3.download_file(segment.get_admin_s3_key(), fp)
        return fp

    def _get_audit(self, segment: CustomSegment) -> AuditProcessor:
        """
        Try and get valid existing audit for segment to not waste processing of recent data.
        An existing audit is valid if it has been created less than settings.AUDIT_SDF_VALID_TIME
        :param segment: CustomSegment
        :return: None | AuditProcessor
        """
        try:
            audit_id = segment.params[Params.DV360_SYNC_DATA][Params.META_AUDIT_ID]
            audit = AuditProcessor.objects.get(id=audit_id)
            # Invalid if audit is too old. Data must be refreshed
            if audit.created < now_in_default_tz() - timedelta(hours=settings.AUDIT_SDF_VALID_TIME):
                audit = None
        except (KeyError, AuditProcessor.DoesNotExist):
            audit = None
        return audit
