from operator import attrgetter

from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.api.serializers.audit_vet_channel_serializer import AuditChannelVetSerializer
from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from segment.models import CustomSegment
from utils.permissions import user_has_permission


class AuditVetRetrieveUpdateAPIView(APIView):
    serializer = AuditChannelVetSerializer
    es_sections = (Sections.MAIN, Sections.TASK_US_DATA, Sections.MONETIZATION)
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request, *args, **kwargs):
        """
        Retrieve next item to be vetted in list
        """
        audit_id = kwargs["pk"]
        segment = self._get_segment(audit_id)
        data = self._retrieve_next_list_item(segment)
        return Response(status=HTTP_200_OK, data=data)

    def patch(self, request, *args, **kwargs):
        """
        Save vetting data
        """
        audit_id = kwargs["pk"]
        segment = self._get_segment(audit_id)
        data = request.data
        skipped = data.get("skipped")
        vetting_model = segment.audit_utils.vetting_model
        try:
            vetting_id = data["vetting_id"]
            vetting_item = vetting_model.objects.get(id=vetting_id)
        except KeyError:
            raise ValidationError("You must provide a vetting_id.")
        except vetting_model.DoesNotExist:
            raise ValidationError("Vetting item does not exist.")

        data["is_checked_out"] = vetting_item.is_checked_out = False
        data["processed"] = vetting_item.processed = timezone.now()
        data["processed_by_user_id"] = vetting_item.processed_by_user_id = request.user.id
        data["suitable"] = data["suitable"]
        if skipped:
            self._processs_skipped(skipped, vetting_item)
        else:
            serializer = self.serializer(vetting_item, data=data, segment=segment)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        return Response(status=HTTP_200_OK)

    def _retrieve_next_list_item(self, segment):
        """
        Retrieve next item to vet in segment
        :param user: UserProfile
        :param segment_id: int
        :return: dict | str
        """
        # id_key = video.video_id, channel.channel_id
        id_key = segment.data_field + "." + segment.data_field + "_id"
        audit = AuditProcessor.objects.get(id=segment.audit_id, source=1)
        next_item = segment.audit_utils.vetting_model.objects.filter(audit=audit, is_checked_out=False, processed=None).first()
        # If next item is None, then all are checked out
        if next_item:
            item_id = attrgetter(id_key)(next_item)
            segment.es_manager.sections = self.es_sections
            response = segment.es_manager.get([item_id])[0]
            data = AuditChannelVetSerializer(response, segment=segment).data
            data["vetting_id"] = next_item.id
            next_item.is_checked_out = True
            next_item.checked_out_at = timezone.now()
            next_item.save()
        else:
            data = "All items are currently being vetted."
        return data

    def _get_segment(self, audit_id):
        """
        Retrieve CustomSegment. Raises ValidationError if not found
        :param audit_id: int
        :return:
        """
        try:
            segment = CustomSegment.objects.get(audit_id=audit_id)
        except CustomSegment.DoesNotExist:
            raise ValidationError(f"Segment with audit_id: {audit_id} not found.", code=HTTP_404_NOT_FOUND)
        return segment

    def _processs_skipped(self, skipped_type, vetting_item):
        """
        Handle user skipping vetting for item
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        :param skipped_type:
        :param vetting_item:
        :return:
        """
        vetting_item.clean = False
        if skipped_type == 0:
            # doesnt exist
            vetting_item.skipped = False
        elif skipped_type == 1:
            vetting_item.skipped = True
        else:
            raise ValidationError(f"Invalid skip type. Must be 0-1, inclusive.")
        vetting_item.save()
