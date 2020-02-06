from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.api.serializers.audit_vet_video_serializer import AuditVetSerializer
from audit_tool.models import AuditProcessor
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.constants import Sections
from segment.models import CustomSegment


class AuditVetRetrieveUpdateAPIView(APIView):
    serializer = AuditVetSerializer

    def get(self, request, *args, **kwargs):
        """
        Retrieve next item to be vetted in list
        """
        segment_id = request.query_params.get("segment_id")
        data = self._retrieve_next_list_item(request.user, segment_id)
        return Response(status=HTTP_200_OK, data=data)

    def post(self, request, *args, **kwargs):
        """
        Save vetting data
        """
        audit_id = kwargs["audit_id"]
        vetting_id = kwargs["vetting_id"]
        data = request.data
        try:
            audit = AuditProcessor.objects.get(id=audit_id)
        except AuditProcessor.DoesNotExist:
            raise ValidationError(f"Audit with id: {audit_id} does not exist.")
        vetting_model = audit.segment.audit_vetting_model
        try:
            vetting_item = vetting_model.objects.get(id=vetting_id)
        except vetting_model.DoesNotExist:
            raise ValidationError("Vetting item does not exist.")
        vetting_item.checked_out_by = None
        serializer = self.serializer(data)
        serializer.is_valid()
        serializer.update()
        doc = Channel(serializer.data)
        ChannelManager(upsert_sections=(Sections.TASK_US_DATA,)).upsert([doc])
        return Response(status=HTTP_201_CREATED)

    def _retrieve_next_list_item(self, user, segment_id):
        """
        Retrieve next item to vet in segment
        :param user: UserProfile
        :param segment_id: int
        :return: dict | str
        """
        try:
            segment = CustomSegment.objects.get(id=segment_id.get("id"))
        except CustomSegment.DoesNotExist:
            raise ValidationError(f"Segment with id: {segment_id} not found.", code=HTTP_404_NOT_FOUND)

        audit = AuditProcessor.objects.get(segment=segment, audit_type=3)
        next_item = segment.vetting_model.objects.filter(audit=audit, checked_out_by=None).order_by("cursor").first()
        # If next item is None, then all are checked out
        if next_item:
            next_item.checked_out_by = user
            next_item.last_checked_out_at = timezone.now()
            serializer = self.serializer(next_item)
            serializer.save()
            data = serializer.data
        else:
            data = "All items are currently being vetted."
        return data
