from operator import attrgetter

from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from segment.models import CustomSegment
from utils.permissions import user_has_permission
from utils.views import get_object
from utils.views import validate_fields


class AuditVetRetrieveUpdateAPIView(APIView):
    ES_SECTIONS = (Sections.MAIN, Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.MONETIZATION)
    REQUIRED_FIELDS = ("age_group", "brand_safety", "content_type", "gender", "iab_categories",
                       "is_monetizable", "language", "vetting_id", "suitable", "language")
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request, *args, **kwargs):
        """
        Retrieve next item to be vetted in list
        """
        audit_id = kwargs["pk"]
        params = {"audit_id": audit_id}
        segment = get_object(CustomSegment, f"Segment with audit_id: {audit_id} not found.", **params)
        try:
            data = self._retrieve_next_list_item(segment)
        except MissingDocumentException:
            data = 'The item you requested has been deleted. ' \
                   'Please save the item as "skipped" with option: "Doesn\'t Exist'
        return Response(status=HTTP_200_OK, data=data)

    def patch(self, request, *args, **kwargs):
        """
        Save vetting data
        """
        audit_id = kwargs["pk"]
        params = {"audit_id": audit_id}
        segment = get_object(CustomSegment, f"Segment with audit_id: {audit_id} not found.", **params)
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

        data["checked_out_at"] = vetting_item.checked_out_at = None
        data["processed"] = vetting_item.processed = timezone.now()
        data["processed_by_user_id"] = vetting_item.processed_by_user_id = request.user.id
        if skipped:
            self._processs_skipped(skipped, vetting_item)
            res = None
        else:
            validate_fields(self.REQUIRED_FIELDS, list(data.keys()))
            data["suitable"] = data["suitable"]
            # Rename field for validation since language is used as SerializerMethodField for Elasticsearch
            # serialization and SerializerMethodField is read only
            data["language_code"] = data.pop("language")
            serializer = segment.audit_utils.serializer(vetting_item, data=data, segment=segment)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            res = serializer.validated_data
        return Response(status=HTTP_200_OK, data=res)

    def _retrieve_next_list_item(self, segment):
        """
        Retrieve next item to vet in segment
        :param user: UserProfile
        :param segment_id: int
        :return: dict | str
        """
        # id_key = video.video_id, channel.channel_id
        id_key = segment.data_field + "." + segment.data_field + "_id"
        params = {"id": segment.audit_id, "source": 1}
        audit = get_object(AuditProcessor, f"Audit with id: {segment.audit_id} not found", **params)
        next_item = segment.audit_utils.vetting_model.objects.filter(audit=audit, checked_out_at=None, processed=None).first()
        # If next item is None, then all are checked out
        if next_item:
            item_id = attrgetter(id_key)(next_item)
            segment.es_manager.sections = self.ES_SECTIONS
            response = self._get_document(segment.es_manager, item_id)
            data = segment.audit_utils.serializer(response, segment=segment).data
            data["vetting_id"] = next_item.id
            data["checked_out_at"] = next_item.checked_out_at = timezone.now()
            next_item.save()
        else:
            data = "All items are currently being vetted."
        return data

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

    def _get_document(self, es_manager, item_id):
        """
        Handle retrieving Elasticsearch document
        In some cases an item was avaiable during list creation was deleted before vetting could take place.
        Respond with prompt to save item as skipped
        :param es_manager:
        :param item_id:
        :return:
        """
        try:
            document = es_manager.get([item_id])[0]
        except IndexError:
            raise MissingDocumentException
        return document


class MissingDocumentException(Exception):
    pass
