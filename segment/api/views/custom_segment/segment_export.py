from django.http import StreamingHttpResponse
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from audit_tool.utils.audit_utils import AuditUtils
from segment.models import CustomSegment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from utils.aws.s3_exporter import ReportNotFoundException
from utils.permissions import user_has_permission
from utils.views import get_object


class SegmentExport(APIView):
    permission_classes = (
        user_has_permission("userprofile.vet_audit_admin"),
    )

    def get(self, request, pk, *_):
        segment = get_object(CustomSegment, f"Custom segment with id: {pk} not found.", id=pk)
        segment.set_vetting()
        if request.query_params.get("vetted"):
            # Get completed vetted list if available
            try:
                s3_key = segment.get_vetted_s3_key()
                content_generator = segment.get_export_file(s3_key=s3_key)
            except ReportNotFoundException:
                content_generator = None
        else:
            segment = CustomSegment.objects.get(id=pk)
            content_generator = segment.get_export_file()
        if content_generator:
            response = StreamingHttpResponse(
                content_generator,
                content_type="application/CSV",
                status=HTTP_200_OK,
            )
            filename = "{}.csv".format(segment.title)
            response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        else:
            # If no export, generate and email result
            generate_vetted_segment.delay(segment.id, recipient=request.user.email)
            response = Response({
                "message": f"Processing. You will receive an email when your export for: {segment.title} is ready.",
            })
        return response


class DynamicGenerationLimitExceeded(Exception):
    """ Exception to raise if export is too large to dynamically generate """
    pass
