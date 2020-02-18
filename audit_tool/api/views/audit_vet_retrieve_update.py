from operator import attrgetter

from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from datetime import timedelta
from es_components.constants import Sections
from segment.models import CustomSegment
from utils.permissions import user_has_permission
from utils.views import get_object
from utils.views import validate_fields
from django.db.models import Q

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
        params = {"id": segment.audit_id, "source": 1}
        audit = get_object(AuditProcessor, f"Audit with id: {segment.audit_id} not found", **params)
        if audit.completed is None:
            raise ValidationError("Vetting for this list is not ready, audit still running.")
        try:
            data = self._retrieve_next_vetting_item(segment, audit)
        except MissingDocumentException:
            data = 'The item you requested has been deleted. ' \
                   'Please save the item as "skipped" with option: "Doesn\'t Exist'
        return Response(status=HTTP_200_OK, data=data)

    def patch(self, request, *args, **kwargs):
        """
        Save vetting data
        """
        audit_id = kwargs["pk"]
        data = request.data
        vetting_item, segment, skipped = self._validate_patch_params(audit_id, data)
        data["checked_out_at"] = vetting_item.checked_out_at = None
        data["processed"] = vetting_item.processed = timezone.now()
        data["processed_by_user_id"] = vetting_item.processed_by_user_id = request.user.id
        if skipped is not None:
            self._processs_skipped(skipped, vetting_item)
            res = None
        else:
            validate_fields(self.REQUIRED_FIELDS, list(data.keys()))
            # Rename field for validation since language is used as SerializerMethodField for Elasticsearch
            # serialization and SerializerMethodField is read only
            data["language_code"] = data.pop("language")
            serializer = segment.audit_utils.serializer(vetting_item, data=data, segment=segment)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            res = serializer.validated_data
            res["task_us_data"]["language"] = res.pop("language_code")
        return Response(status=HTTP_200_OK, data=res)

    def _validate_patch_params(self, audit_id, data):
        """
        Method to validate patch payload data
        Raises ValidationErrors if invalid parameters
        :param audit_id: int
        :param data: dict
        :return: tuple
        """
        params = {"audit_id": audit_id}
        segment = get_object(CustomSegment, f"Segment with audit_id: {audit_id} not found.", **params)
        skipped = data.get("skipped")
        vetting_model = segment.audit_utils.vetting_model
        try:
            vetting_id = data["vetting_id"]
            vetting_item = vetting_model.objects.get(id=vetting_id)
        except KeyError:
            raise ValidationError("You must provide a vetting_id.")
        except vetting_model.DoesNotExist:
            raise ValidationError("Vetting item does not exist.")
        if vetting_item.processed is not None:
            raise ValidationError("Item has been vetted. Please continue to the next item.")
        return vetting_item, segment, skipped

    def _retrieve_next_vetting_item(self, segment, audit):
        """
        Retrieve next item to vet in segment
        :param segment: CustomSegment
        :param audit: AuditProcesssor
        :return: dict | str
        """
        # id_key = video.video_id, channel.channel_id
        id_key = segment.data_field + "." + segment.data_field + "_id"
        next_item = segment.audit_utils.vetting_model.objects.filter(audit=audit, processed__isnull=True).filter(Q(checked_out_at__isnull=True) | Q(checked_out_at__lt=timezone.now()-timedelta(minutes=30))).first()
        # If next item is None, then all are checked out
        if next_item:
            item_id = attrgetter(id_key)(next_item)
            segment.es_manager.sections = self.ES_SECTIONS
            response = self._get_document(segment.es_manager, item_id)
            data = segment.audit_utils.serializer(response, segment=segment).data
            data["vetting_id"] = next_item.id
            data['data_type'] = segment.data_field
            data["checked_out_at"] = next_item.checked_out_at = timezone.now()
            data["instructions"] = audit.params.get("instructions")
            next_item.save()
        else:
            raise ValidationError("All items are checked out. Please request from a different list.")
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
