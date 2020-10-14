from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.models import CustomSegment
from utils.permissions import user_has_permission


class PersistentSegmentExportApiView(DynamicPersistentModelViewMixin, APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get(self, request, pk, *_):
        try:
            segment = CustomSegment.objects.get(id=pk)
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
