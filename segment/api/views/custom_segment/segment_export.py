from rest_framework.response import Response
from rest_framework.views import APIView

from segment.models import CustomSegment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from segment.utils.utils import CustomSegmentOwnerPermission
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.views import get_object


class SegmentExport(APIView):
    permission_classes = (
        or_permission_classes(
            CustomSegmentOwnerPermission,
            user_has_permission("userprofile.vet_audit_admin")
        ),
    )

    def get(self, request, pk, *_):
        segment = get_object(CustomSegment, f"Custom segment with id: {pk} not found.", id=pk)
        response = {}
        if request.query_params.get("vetted"):
            s3_key = segment.get_vetted_s3_key()
            if hasattr(segment, "vetted_export") and segment.s3.exists(s3_key, get_key=False):
                response["download_url"] = segment.s3.generate_temporary_url(s3_key)
            else:
                generate_vetted_segment.delay(segment.id, recipient=request.user.email)
                response[
                    "message"] = f"Processing. You will receive an email when your export for: {segment.title} is " \
                                 f"ready."
        else:
            if hasattr(segment, "export"):
                s3_key = segment.get_s3_key()
                response["download_url"] = segment.s3.generate_temporary_url(s3_key)
            else:
                response["message"] = "Segment has no export. Please create the list again."
        return Response(response)


class DynamicGenerationLimitExceeded(Exception):
    """ Exception to raise if export is too large to dynamically generate """
