from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from audit_tool.utils.audit_utils import AuditUtils
from utils.views import get_object


class AuditAdminAPIView(APIView):
    permission_classes = (IsAdminUser,)

    def patch(self, request, *args, **kwargs):
        """
        Update vetting items that should be re-vetted
        """
        data = request.data
        audit_id = data["audit_id"]
        params = {"id": audit_id}
        audit = get_object(AuditProcessor, f"Audit with id: {audit_id} not found.", **params)
        audit_utils = AuditUtils(audit.audit_type)
        item_ids = data.get("item_ids", [])
        validated = self._validate_item_ids(item_ids, audit.audit_type)
        audit_utils.vetting_model.objects\
            .filter(audit=audit, channel__channel_id__in=validated)\
            .update(processed=None, clean=None)
        audit.completed = None
        audit.save()
        return Response(status=HTTP_200_OK, data=validated)

    def _validate_item_ids(self, item_ids, audit_type):
        """
        Validate item id lengths based on audit_type
        Raises ValidationError if any invalid id is encountered
        :param item_ids: list
        :param audit_type: int
        :return: item_ids
        """
        err_suffix = " Please check ids and resubmit."
        err = None
        if audit_type == 1:
            invalid = any(type(item) is not str or len(item) > 11 for item in item_ids)
            if invalid:
                err = "Invalid video ids." + err_suffix
        elif audit_type == 2:
            invalid = any(type(item) is not str or len(item) < 24 for item in item_ids)
            if invalid:
                err = "Invalid channel ids." + err_suffix
        else:
            err = f"Invalid audit_type: {audit_type}"
        if err:
            raise ValidationError(err)
        return item_ids
