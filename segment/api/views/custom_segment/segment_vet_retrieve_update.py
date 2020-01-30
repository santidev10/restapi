import json

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVetItem
from segment.models import CustomSegment
from audit_tool.api.serializers.audit_vet_serializer import AuditVetSerializer


class SegmentVetRetrieveUpdateAPIView(APIView):
    serializer = AuditVetSerializer

    def get(self, request, *args, **kwargs):
        try:
            segment = CustomSegment.objects.get(id=kwargs.get("id"))
        except CustomSegment.DoesNotExist:
            raise 404

        if segment.audit is None:
            audit = AuditProcessor.objects.create(segment=segment, audit_type=3)
        else:
            audit = segment.audit

        next_item = AuditVetItem.objects.filter(segment=segment, checked_out_by=None).order_by("cursor").first()
        # If next item is None, then all are checked out
        if next_item:
            data = self.serializer(next_item).data
        else:
            data = "All items are currently being vetted."
        return Response(status=HTTP_200_OK, data=data)

    def post(self, request, *args, **kwargs):
        # save data to audit processor
        # check if item cursor id is the next of the audit processor cursor
        # remove the item from the cache
        segment = CustomSegment.objects.get(id=request.data["id"])
        cursor = request.data["cursor"]
        item = AuditVetItem.objects.get(cursor=cursor, segment=segment)
        if not item.exists():
            raise ValidationError(f"Item with cursor: {cursor} does not exist.")
        serializer = self.serializer(item)
        serializer.is_valid()
        serializer.save()
        return Response(status=HTTP_201_CREATED)
