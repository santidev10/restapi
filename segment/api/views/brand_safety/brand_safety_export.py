from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from utils.permissions import user_has_permission


class PersistentSegmentExportApiView(DynamicPersistentModelViewMixin, APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get(self, request, pk, *_):
        try:
            segment = self.get_queryset().get(pk=pk)
            content_generator = segment.get_s3_export_content().iter_chunks()
        except segment.__class__.DoesNotExist:
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