from datetime import timedelta
from operator import attrgetter

from django.db.models import Q
from django.utils import timezone
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditProcessor
from audit_tool.validators import AuditToolValidator
from es_components.constants import Sections
from segment.models import CustomSegment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from segment.tasks import update_segment_statistics
from userprofile.constants import StaticPermissions
from utils.views import get_object
from utils.views import validate_fields


CHECKOUT_THRESHOLD = 10

class AuditVetRetrieveUpdateAPIView(APIView):
    ES_SECTIONS = (Sections.MAIN, Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.MONETIZATION)
    REQUIRED_FIELDS = ("age_group", "brand_safety", "content_type", "content_quality", "gender", "iab_categories",
                       "language", "vetting_id", "suitable", "language")

    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_VET),
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
            if segment.is_vetting_complete is True:
                raise VettingCompleteException
            else:
                data = self._retrieve_next_vetting_item(segment, audit)
        except VettingCompleteException:
            data = {
                "message": "Vetting for this list is complete. Please move on to the next list."
            }
        except MissingItemException as e:
            data = {
                "message": 'The item you requested has been deleted. ' \
                           'Please save the item as "skipped" with option: "Doesn\'t Exist',
                "vetting_id": e.vetting_id,
            }
        return Response(status=HTTP_200_OK, data=data)

    def patch(self, request, *args, **kwargs):
        """
        Save vetting data
        """
        audit_id = kwargs["pk"]
        data = request.data
        vetting_item, segment, audit, skipped = self._validate_patch_params(audit_id, data)
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
            data["lang_code"] = data.pop("language")
            context = {
                "user": request.user,
                "segment": segment,
            }
            serializer = segment.audit_utils.serializer(vetting_item, data=data, context=context)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            res = serializer.validated_data

        # Check vetting completion
        if not segment.audit_utils.vetting_model.objects.filter(audit=audit, processed__isnull=True).exists():
            segment.is_vetting_complete = True
            audit.completed = timezone.now()
            audit.save()
            segment.save()
            generate_vetted_segment.delay(segment.id)
            update_segment_statistics.delay([segment.id])
        return Response(status=HTTP_200_OK, data=res)

    def _validate_patch_params(self, audit_id, data):
        """
        Method to validate patch payload data
        Raises ValidationErrors if invalid parameters
        :param audit_id: int
        :param data: dict
        :return: tuple
        """
        segment_params = {"audit_id": audit_id}
        segment = get_object(CustomSegment, f"Segment with audit_id: {audit_id} not found.", **segment_params)
        audit_params = {"id": audit_id}
        audit = get_object(AuditProcessor, f"Audit with audit_id: {audit_id} not found.", **audit_params)
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
        return vetting_item, segment, audit, skipped

    def _retrieve_next_vetting_item(self, segment, audit):
        """
        Retrieve next item to vet in segment
        :param segment: CustomSegment
        :param audit: AuditProcesssor
        :return: dict | str
        """
        # id_key = video.video_id, channel.channel_id
        id_key = segment.config.DATA_FIELD + "." + segment.config.DATA_FIELD + "_id"
        # get the next vetting model for this audit, that wasn't processed, that has either: never been checked out OR
        # was checked out over CHECKOUT_THRESHOLD minutes ago
        next_item = segment.audit_utils.vetting_model.objects.filter(audit=audit, processed__isnull=True).filter(
            Q(checked_out_at__isnull=True)
            | Q(checked_out_at__lt=timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD))
        ).first()
        # If next item is None, then all are checked out
        if next_item:
            try:
                item_id = attrgetter(id_key)(next_item)
            except AttributeError:
                raise MissingItemException(next_item.id)
            segment.es_manager.sections = self.ES_SECTIONS
            response = self._get_document(segment.es_manager, item_id, next_item.id)
            data = segment.audit_utils.serializer(response, context={"segment": segment}).data
            data["vetting_id"] = next_item.id
            if response:
                data["title"] = response.general_data.title
                data['data_type'] = segment.config.DATA_FIELD
            data["suitable"] = next_item.clean
            data["checked_out_at"] = next_item.checked_out_at = timezone.now()
            data["instructions"] = audit.params.get("instructions")
            data['iab_categories'] = self.filter_invalid_iab_categories(data['iab_categories'])
            try:
                o = getattr(next_item, segment.config.DATA_FIELD)
                data['YT_id'] = getattr(o, "{}_id".format(segment.config.DATA_FIELD))
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            next_item.save(update_fields=['checked_out_at'])
            # Allow client to refresh page with new vetting id. Client will experience a 400 error if attempting to
            # update an already submitted vetting id, as other clients may receive the same vetting id if CHECKOUT_THRESHOLD
            # has elapsed
            data["expires"] = CHECKOUT_THRESHOLD
        else:
            data = {
                "message": "All items are checked out. Please request from a different list."
            }
        return data

    def filter_invalid_iab_categories(self, categories: list) -> list:
        """
        remove invalid iab categories from the passed list of categories
        """
        valid_categories = []
        for category in categories:
            try:
                category_as_list = AuditToolValidator.validate_iab_categories([category])
            except ValidationError:
                continue
            valid_categories = valid_categories + category_as_list
        return valid_categories

    def _processs_skipped(self, skipped_type, vetting_item):
        """
        Handle user skipping vetting for item
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        :param skipped_type: int
        :param vetting_item: audit_tool AuditChannelVet, AuditVideoVet
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

    def _get_document(self, es_manager, item_id, vetting_id):
        """
        Handle retrieving Elasticsearch document
        In some cases an item was available during list creation was deleted before vetting could take place or
            has invalid item_id
        Respond with prompt to save item as skipped
        :param es_manager: es_components ChannelManager, VideoManager
        :param item_id: str
        :return:
        """
        try:
            document = es_manager.get([item_id])[0]
            if not document:
                raise NotFoundError
        except (IndexError, NotFoundError, RequestError):
            raise MissingItemException(vetting_id)
        return document


class MissingItemException(Exception):
    def __init__(self, vetting_id):
        super().__init__()
        self.vetting_id = vetting_id


class VettingCompleteException(Exception):
    pass
