import json

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVetItem
from audit_tool.api.serializers.audit_vet_serializer import AuditVetSerializer
from segment.models import CustomSegment
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.constants import Sections


class AuditVetRetrieveUpdateAPIView(APIView):
    serializer = AuditVetSerializer

    def get(self, request, *args, **kwargs):
        # Will be coming from segment page, so use segment id
        try:
            segment = CustomSegment.objects.get(id=kwargs.get("id"))
        except CustomSegment.DoesNotExist:
            raise 404

        audit, created = AuditProcessor.objects.get_or_create(segment=segment, audit_type=3)
        if created is True:
            # If starting vetting for the first time, retrieve ids in list to create AuditVetItems for
            item_ids = segment.get_extract_export_ids()
            vet_items = [AuditVetItem(id=_id, audit=audit) for _id in item_ids]
            AuditVetItem.objects.bulk_create(vet_items)
            next_item = vet_items[0]
        else:
            next_item = AuditVetItem.objects.filter(audit=audit, checked_out_by=None).order_by("cursor").first()
        # If next item is None, then all are checked out
        if next_item:
            next_item.checked_out_by = request.user
            serializer = self.serializer(next_item)
            serializer.save()
            data = serializer.data
        else:
            data = "All items are currently being vetted."

        return Response(status=HTTP_200_OK, data=data)

    def post(self, request, *args, **kwargs):
        # will be coming from audit page, so use audit id
        audit_id = kwargs["audit_id"]
        vet_id = kwargs["vet_id"]
        try:
            audit = AuditProcessor.objects.get(id=audit_id)
        except AuditProcessor.DoesNotExist:
            raise ValidationError(f"Audit with id: {audit_id} does not exist.")
        item = AuditVetItem.objects.get(id=vet_id)
        serializer = self.serializer(item)
        serializer.is_valid()
        serializer.save()
        doc = Channel(serializer.data)
        ChannelManager(upsert_sections=(Sections.TASK_US_DATA,)).upsert([doc])
        return Response(status=HTTP_201_CREATED)
