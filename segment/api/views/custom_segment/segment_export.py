from distutils.util import strtobool

from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied
from rest_framework.views import APIView

from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models.utils.segment_action import segment_action
from segment.models.constants import SegmentActionEnum
from segment.models.constants import VideoExclusion
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from segment.utils.utils import AdminCustomSegmentOwnerPermission
from utils.permissions import or_permission_classes
from userprofile.constants import StaticPermissions
from utils.views import get_object


class SegmentExport(APIView):
    permission_classes = (
        or_permission_classes(
            AdminCustomSegmentOwnerPermission,
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_EXPORT_BASIC, StaticPermissions.BUILD__CTL_EXPORT_ADMIN,
                                        StaticPermissions.BUILD__CTL_VET_EXPORT)
        ),
    )

    @segment_action(SegmentActionEnum.DOWNLOAD.value)
    def get(self, request, pk, *_):
        segment = get_object(CustomSegment, f"Custom segment with id: {pk} not found.", id=pk)
        response = {}
        if request.query_params.get("vetted"):
            if not request.user.has_permission(StaticPermissions.BUILD__CTL_VET_EXPORT):
                raise PermissionDenied

            s3_key = segment.get_vetted_s3_key()
            if hasattr(segment, "vetted_export") and segment.s3.exists(s3_key, get_key=False):
                response["download_url"] = segment.s3.generate_temporary_url(s3_key)
            else:
                generate_vetted_segment.delay(segment.id, recipient=request.user.email)
                response[
                    "message"] = f"Processing. You will receive an email when your export for: {segment.title} is " \
                                 f"ready."
        else:
            if strtobool(request.query_params.get("video_exclusion")):
                if not request.user.has_permission(StaticPermissions.BUILD__CTL_VIDEO_EXCLUSION):
                    raise PermissionDenied
                video_exclusion_ctl = get_object(CustomSegment, id=segment.statistics.get(VideoExclusion.VIDEO_EXCLUSION_ID))
                s3_key = video_exclusion_ctl.export.filename
                response["download_url"] = segment.s3.generate_temporary_url(s3_key)
            elif hasattr(segment, "export"):
                related_file_obj = get_object(CustomSegmentFileUpload, f"CustomSegmentFileUpload obj with " \
                                            f"segment_id: {segment.id} not found.", segment_id=segment.id)
                if request.user.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
                    if related_file_obj.admin_filename:
                        admin_s3_key = segment.get_admin_s3_key()
                        response["download_url"] = segment.s3.generate_temporary_url(admin_s3_key)
                    else:
                        s3_key = segment.get_s3_key()
                        response["download_url"] = segment.s3.generate_temporary_url(s3_key)
                else:
                    s3_key = segment.get_s3_key()
                    response["download_url"] = segment.s3.generate_temporary_url(s3_key)
            else:
                response["message"] = "Segment has no export. Please create the list again."
        return Response(response)


class DynamicGenerationLimitExceeded(Exception):
    """ Exception to raise if export is too large to dynamically generate """
