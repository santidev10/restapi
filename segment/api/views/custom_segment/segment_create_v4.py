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
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from segment.utils.query_builder import SegmentQueryBuilder


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
        query_builder = SegmentQueryBuilder(validated_data)
        # Use query_builder.query_params to get mapped values used in Elasticsearch query
        query = {
            "params": query_builder.query_params,
            "body": query_builder.query_body.to_dict()
        }
        CustomSegmentFileUpload.enqueue(query=query, segment=segment)
        generate_custom_segment.delay(segment.id)
        res = self._get_response(query_builder.query_params, segment)
        return Response(status=HTTP_201_CREATED, data=res)

    def _validate_data(self, request, data):
        """
        Validate request data
        Raise ValidationError on invalid parameters
        :param user_id: int
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
        serializer = self.serializer_class(data=data, context=dict(request=self.request))
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        return segment

    def _get_response(self, params, segment):
        """
        Copy params with additional fields for response
        :param segment: CustomSegment
        :param params: dict
        :return:
        """
        res = params.copy()
        res["id"] = segment.id
        res["title"] = segment.title
        res["segment_type"] = segment.segment_type
        res["pending"] = True
        res["statistics"] = {}
        return res


class SegmentCreationOptionsError(Exception):
    pass
