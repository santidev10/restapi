import json

from django.core.files.uploadhandler import TemporaryFileUploadHandler
from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import CreateAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_200_OK

from segment.api.serializers import CTLParamsSerializer
from segment.api.serializers.ctl_serializer import CTLSerializer
from segment.api.serializers.video_exclusion_params_serializer import VideoExclusionParamsSerializer
from segment.api.mixins import SegmentTypePermissionMixin
from segment.models import CustomSegment
from segment.models.constants import SegmentActionEnum
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import Params
from segment.models.utils.segment_action import segment_action
from segment.utils.utils import set_user_perm_params
from userprofile.constants import StaticPermissions
from utils.permissions import or_permission_classes
from utils.views import get_object


class SegmentCreateUpdateApiView(CreateAPIView, SegmentTypePermissionMixin):
    serializer_class = CTLSerializer
    permission_classes = (
        or_permission_classes(
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST),
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST)
        ),
    )
    parser_classes = [MultiPartParser]
    permission_by_segment_type = {
        SegmentTypeEnum.VIDEO.value: StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST,
        SegmentTypeEnum.CHANNEL.value: StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST
    }

    def _prep_request(self, request):
        request.upload_handlers = [TemporaryFileUploadHandler(request)]
        data = json.loads(request.data["data"])

        data = set_user_perm_params(request, data)
        return request, data

    @staticmethod
    def check_source_file_permissions(request):
        """
        ensure user has permission to upload a CTL source file
        :param request:
        :return:
        """
        if request.FILES.get("source_file") \
                and not request.user.has_permission(StaticPermissions.BUILD__CTL_FROM_CUSTOM_LIST):
            raise PermissionDenied

    @segment_action(SegmentActionEnum.CREATE.value)
    def post(self, request, *args, **kwargs):
        """
        Create CustomSegment, CustomSegmentFileUpload, and execute generate_custom_segment task through CTLSerializer
        """
        request, data = self._prep_request(request)
        validated_params = self._validate_params(data)
        self.check_segment_type_permissions(request=request, segment_type=validated_params.get("segment_type"))
        self.check_source_file_permissions(request=request)
        serializer = self.serializer_class(data=data, context=self._get_context(validated_params))
        res = self._finalize(serializer, validated_params)
        return Response(status=HTTP_201_CREATED, data=res)

    def patch(self, request, *args, **kwargs):
        """
        Update CustomSegment, and update CustomSegmentFileUpload and execute generate_custom_segment task
            if necessary through CTLSerializer. If any files or CTL filters change during update, the list will
            be regenerated with updated values
        """
        request, data = self._prep_request(request)
        segment = get_object(CustomSegment, id=data.get("id"))
        self.check_segment_type_permissions(request=request, segment_type=segment.segment_type, allow_if_owner=True,
                                            segment=segment)
        self.check_source_file_permissions(request=request)
        # Keep track of data.keys as CTLParamsSerializer sets default values for some fields during creation.
        # validated_params will need to be cleaned of these default values and only the keys send for updating should
        # be included in context
        data_keys = set(data.keys())
        validated_params = self._validate_params(data, partial=True)
        validated_video_exclusion_params = self._validate_video_exclusion(segment, request.user, data)
        cleaned_params = {key: value for key, value in validated_params.items() if key in data_keys}
        serializer = self.serializer_class(segment, data=data,
                                           context=self._get_context(cleaned_params, validated_video_exclusion_params), partial=True)
        res = self._finalize(serializer, validated_params)
        return Response(status=HTTP_200_OK, data=res)

    def _finalize(self, serializer, validated_ctl_params):
        """
        Validate CTLSerializer and get response data
        :param serializer: CTLSerializer
        :param validated_ctl_params: CTLParamsSerializer validated data
        :return: dict
        """
        serializer.is_valid(raise_exception=True)
        serializer.save()
        serializer.data.update(validated_ctl_params)
        return serializer.data

    def _validate_params(self, data, partial=False):
        """
        Validate request data
        :param data: dict
        :return: dict
        """
        params_serializer = CTLParamsSerializer(data=data, partial=partial)
        params_serializer.is_valid(raise_exception=True)
        validated_data = params_serializer.validated_data
        return validated_data

    def _validate_video_exclusion(self, ctl, user, data):
        """
        Validate permissions and serializer values for creating video exclusion for ctl. If exclusion params is
            provided, then it is assumed that the ctl is a channel ctl and only a video exclusion list should be
            created for it.
        :param user: UserProfile
        :param data: dict
        :return:
        """
        if data.get(Params.VideoExclusion.WITH_VIDEO_EXCLUSION) is True:
            if not user.has_permission(StaticPermissions.BUILD__CTL_VIDEO_EXCLUSION):
                raise PermissionDenied
            video_exclusion_serializer = VideoExclusionParamsSerializer(data=data, context={"source_ctl": ctl})
            video_exclusion_serializer.is_valid(raise_exception=True)
            exclusion_params = video_exclusion_serializer.validated_data
        else:
            exclusion_params = {}
        return exclusion_params

    def _get_context(self, data, video_exclusion_params=None):
        context = {
            "request": self.request,
            "ctl_params": data,
            "files": self.request.FILES,
            "video_exclusion_params": video_exclusion_params
        }
        return context

