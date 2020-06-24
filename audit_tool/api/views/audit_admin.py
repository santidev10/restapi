from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditProcessor
from segment.models import CustomSegment
from segment.models import CustomSegmentVettedFileUpload
from utils.permissions import user_has_permission
from utils.views import get_object


class AuditAdminAPIView(APIView):
    permission_classes = (
        user_has_permission("userprofile.vet_audit_admin"),
    )

    def patch(self, request, *_, **__):
        """
        Update vetting items that should be re-vetted
        """
        data = request.data
        audit_id = data["audit_id"]
        audit_params = {"id": audit_id}
        segment_params = {"audit_id": audit_id}
        audit = get_object(AuditProcessor, f"Audit with id: {audit_id} not found.", **audit_params)
        segment = get_object(CustomSegment, f"Segment with audit id: {audit_id} not found.", **segment_params)
        item_ids = data.get("items_ids", "")
        update_filter = self._validate_item_ids(item_ids, audit.audit_type)
        to_update = segment.audit_utils.vetting_model.objects \
            .filter(audit=audit, **update_filter)
        if to_update.exists():
            to_update.update(processed=None, clean=None)
            segment.is_vetting_complete = False
            segment.save()
            try:
                segment.vetted_export.delete()
            except CustomSegmentVettedFileUpload.DoesNotExist:
                pass
        return Response(status=HTTP_200_OK)

    def _validate_item_ids(self, item_ids, audit_type):
        """
        Validate item id lengths based on audit_type
        Raises ValidationError if any invalid id is encountered
        :param item_ids: list
        :param audit_type: int
        :return: item_ids
        """
        filters = {}
        err = None
        split_seq = None
        filter_prefix = None
        id_validator = None
        if audit_type == 1:
            split_seq = "/watch?v="
            filter_prefix = "video__video_id__in"
            id_validator = lambda x: type(x) is not str or len(x) != 11
        elif audit_type == 2:
            split_seq = "/channel/"
            filter_prefix = "channel__channel_id__in"
            id_validator = lambda x: type(x) is not str or len(x) != 24
        else:
            err = f"Invalid audit_type: {audit_type}"

        try:
            item_ids = [_id.strip().split(split_seq)[-1] for _id in item_ids.split("\n") if _id]
            invalid = any(id_validator(item) for item in item_ids)
            if invalid:
                err = f"Invalid urls. Please check that urls match this format: https://www.youtube.com{split_seq}YOUTUBE_ID"
        except (AttributeError, TypeError):
            err = "Each row must contain one item."

        if err:
            raise ValidationError(err)
        filters[filter_prefix] = item_ids
        return filters
