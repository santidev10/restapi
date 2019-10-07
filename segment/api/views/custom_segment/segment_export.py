from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from segment.models import CustomSegment
from segment.custom_segment_export_generator import CustomSegmentExportGenerator


class SegmentExport(APIView):
    def get(self, request, pk, *_):
        try:
            segment = CustomSegment.objects.get(owner=request.user, id=pk)
            content_generator = segment.get_export_file()
        except CustomSegment.DoesNotExist:
            raise Http404

        response = StreamingHttpResponse(
            content_generator,
            content_type="application/CSV",
            status=HTTP_200_OK,
        )
        filename = "{}.csv".format(segment.title)
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response
