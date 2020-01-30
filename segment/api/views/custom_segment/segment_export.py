from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError


from audit_tool.models import AuditVetItem
from segment.models import CustomSegment



class SegmentExport(APIView):
    def get(self, request, pk, *_):
        try:
            segment = CustomSegment.objects.get(owner=request.user, id=pk)
        except CustomSegment.DoesNotExist:
            raise Http404

        if request.query_params.get("vetted"):
            audit = segment.audit
            if audit is None:
                raise ValidationError(f"Segment: {segment.title} does not have an audit.")
            vetted_items = [item.id for item in AuditVetItem.objects.filter(audit=audit, vetted=True)]
            # get elastic search items and create csv
            data = segment.es_manager.get(vetted_items)
            content_generator = segment.serializer(data, many=True).data
        else:
            content_generator = segment.get_export_file()
        response = StreamingHttpResponse(
            content_generator,
            content_type="application/CSV",
            status=HTTP_200_OK,
        )
        filename = "{}.csv".format(segment.title)
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response
