from operator import attrgetter

from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from segment.models import CustomSegment
from utils.api.exceptions import PermissionsError


class SegmentExport(APIView):
    def get(self, request, pk, *_):
        if request.query_params.get("vetted"):
            if not request.user.has_perm("userprofile.vet_audit_admin"):
                raise PermissionsError("You do not have access to download vetted lists.")
            segment = CustomSegment.objects.get(id=pk)
            audit_id = segment.audit_id
            if audit_id is None:
                raise ValidationError(f"Segment: {segment.title} does not have an audit.")
            vetted_item_ids = []
            id_key = segment.data_field + "." + segment.data_field + "_id"
            for item in segment.audit_utils.vetting_model.objects.filter(audit_id=audit_id, processed__isnull=False).select_related(segment.data_field):
                item_id = attrgetter(id_key)(item)
                vetted_item_ids.append(item_id)
            # get elastic search items and create csv
            content_generator = self._vetted_items_generator(segment, vetted_item_ids)
        else:
            try:
                if request.user.has_perm("userprofile.vet_audit_admin"):
                    segment = CustomSegment.objects.get(id=pk)
                else:
                    segment = CustomSegment.objects.get(id=pk, owner=request.user)
            except CustomSegment.DoesNotExist:
                raise Http404
            content_generator = segment.get_export_file()
        response = StreamingHttpResponse(
            content_generator,
            content_type="application/CSV",
            status=HTTP_200_OK,
        )
        filename = "{}.csv".format(segment.title)
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response

    def _vetted_items_generator(self, segment, item_ids):
        """
        Generator to stream response for vetted items
        :param segment: CustomSegment
        :param item_ids: list
        :return: iter
        """
        header = ",".join(segment.serializer.columns) + "\n"
        yield header
        response = segment.es_manager.get(item_ids)
        for item in response:
            data = segment.serializer(item).data
            data["Category"] = f"\"{data['Category']}\""
            row = ",".join([str(col) if col is not None else "" for col in data.values()])
            row += "\n"
            yield row
