from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from userprofile.constants import StaticPermissions
from utils.views import get_object


class PersistentSegmentExportApiView(DynamicPersistentModelViewMixin, APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.CTL__FEATURE_LIST),
    )

    def get(self, request, pk, *_):
        try:
            segment = CustomSegment.objects.get(id=pk)
            related_file_obj = get_object(CustomSegmentFileUpload, f"CustomSegmentFileUpload obj with " \
                                    f"segment_id: {segment.id} not found.", segment_id=segment.id)
            if request.user.is_staff or request.user.has_permission(StaticPermissions.CTL__VET_ADMIN):
                if related_file_obj.admin_filename:
                    content_generator = segment.s3.get_export_file(segment.get_admin_s3_key())
                else:
                    content_generator = segment.s3.get_export_file(segment.get_s3_key())
            else:
                content_generator = segment.s3.get_export_file(segment.get_s3_key())
        except CustomSegment.DoesNotExist:
            raise Http404
        response = StreamingHttpResponse(
            content_generator,
            content_type=segment.export_content_type,
            status=HTTP_200_OK,
        )
        filename = self.get_filename(segment)
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response

    @staticmethod
    def get_filename(segment):
        return "{}.csv".format(segment.title)
