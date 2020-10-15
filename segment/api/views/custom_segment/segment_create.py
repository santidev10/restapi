import json

from django.core.files.uploadhandler import TemporaryFileUploadHandler
from rest_framework.generics import CreateAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import get_hash_name
from segment.api.serializers import CTLParamsSerializer
from segment.api.serializers.ctl_serializer import CTLSerializer
from segment.models.constants import SegmentActionEnum
from segment.models import CustomSegment
from segment.models.utils.segment_action import segment_action
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.views import get_object


class SegmentCreateApiView(CreateAPIView):
    serializer_class = CTLSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.custom_target_list_creation"),
            IsAdminUser
        ),
    )
    parser_classes = [MultiPartParser]

    @segment_action(SegmentActionEnum.CREATE.value)
    def post(self, request, *args, **kwargs):
        """
        Create CustomSegment, CustomSegmentFileUpload, and execute generate_custom_segment
        """
        request.upload_handlers = [TemporaryFileUploadHandler(request)]
        data = json.loads(request.data["data"])
        validated_params = self._validate_params(data)
        segment = self._create_or_update(data, validated_params)
        res = CTLSerializer(segment).data
        res.update(validated_params)
        return Response(status=HTTP_201_CREATED, data=res)

    def _validate_params(self, data):
        """
        Validate request data
        :param data: dict
        :return: dict
        """
        params_serializer = CTLParamsSerializer(data=data)
        params_serializer.is_valid(raise_exception=True)
        validated_data = params_serializer.validated_data
        return validated_data

    def _get_context(self, data):
        context = {
            "request": self.request,
            "ctl_params": data,
            "files": self.request.FILES
        }
        return context

    def _create_or_update(self, request_data, ctl_params: dict) -> CustomSegment:
        """
        Update or create the CTL depending on the presence of an "id" field in the request body with CTLSerializer
        If updating, since we may need to regenerate CTL export with same logic as when creating it, same validation
        and context values may be required for both creating and updating CTLs.
        :param request_data: Original request body data
        :param ctl_params: CTLParamsSerializer validated data
        :return: CustomSegment
        """
        context = self._get_context(ctl_params)
        # Copy data to not mutate context["ctl_params"] data dict
        request_data.update({
            "title_hash": get_hash_name(request_data["title"].lower().strip()),
            "owner_id": self.request.user.id
        })
        serializer = self._get_serializer(request_data, context)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        return segment

    def _get_serializer(self, request_data, context) -> CTLSerializer:
        """
        Instantiate serializer with kwargs depending if updating or creating by presence of "id" key in request_data
        :param request_data: dict -> Request body
        :param context: self._get_context return value
        :return: CTLSerializer
        """
        if "id" in request_data:
            segment = get_object(CustomSegment, id=request_data["id"])
            serializer = self.serializer_class(segment, data=request_data, context=context, partial=True)
        else:
            serializer = self.serializer_class(data=request_data, context=context)
        return serializer
