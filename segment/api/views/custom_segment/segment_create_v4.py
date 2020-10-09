import json

from django.core.files.uploadhandler import TemporaryFileUploadHandler
from rest_framework.generics import CreateAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import get_hash_name
from segment.api.serializers import CTLParamsSerializer
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer

from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class SegmentCreateApiViewV4(CreateAPIView):
    serializer_class = CustomSegmentSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.custom_target_list_creation"),
            IsAdminUser
        ),
    )
    parser_classes = [MultiPartParser]

    def post(self, request, *args, **kwargs):
        """
        Create CustomSegment, CustomSegmentFileUpload, and execute generate_custom_segment
        """
        request.upload_handlers = [TemporaryFileUploadHandler(request)]
        data = json.loads(request.data["data"])
        validated_data = self._validate_data(request, data)
        validated_data.update(request.FILES)
        segment = self._create(validated_data)
        res = CustomSegmentSerializer(segment).data
        res.update(validated_data)
        return Response(status=HTTP_201_CREATED, data=res)

    def _validate_data(self, request, data):
        """
        Validate request data
        Raise ValidationError on invalid parameters
        :param data: dict
        :return: dict
        """
        params_serializer = CTLParamsSerializer(data=data)
        params_serializer.is_valid(raise_exception=True)
        validated_data = params_serializer.validated_data
        validated_data["title_hash"] = get_hash_name(data["title"].lower().strip())
        validated_data["owner_id"] = request.user.id
        return validated_data

    def _create(self, data: dict):
        """
        Create segment with CustomSegmentSerializer
        :param data: dict
        :return: CustomSegment
        """
        # Pass full CTLParamsSerializer as context as well since CustomSegmentSerializer will only use some fields
        # to create instance
        context = {
            "request": self.request,
            "ctl_params": data,
            # Pop file keys as segment and query creation do not require these values
            "files": {
                key: data.pop(key, None) for key in {"source_file", "inclusion_file", "exclusion_file"}
            },
        }
        serializer = self.serializer_class(data=data, context=context)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        return segment


class SegmentCreationOptionsError(Exception):
    pass
