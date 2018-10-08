from django.contrib.auth import get_user_model
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.views import APIView

from segment.models import SegmentChannel
from segment.models import SegmentKeyword
from segment.models import SegmentVideo
from userprofile.api.serializers import UserCreateSerializer
from userprofile.api.serializers import UserSerializer
from userprofile.permissions import PermissionGroupNames


class UserCreateApiView(APIView):
    """
    User list / create endpoint
    """
    permission_classes = tuple()
    serializer_class = UserCreateSerializer
    retrieve_serializer_class = UserSerializer
    queryset = get_user_model()

    def post(self, request):
        """
        Extend post functionality
        """
        serializer = self.serializer_class(
            data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        self.check_user_segment_access(user)
        response_data = self.retrieve_serializer_class(user).data
        return Response(response_data, status=HTTP_201_CREATED)

    def check_user_segment_access(self, user):
        channel_segment_email_lists = SegmentChannel.objects.filter(
            shared_with__contains=[user.email]).exists()
        video_segment_email_lists = SegmentVideo.objects.filter(
            shared_with__contains=[user.email]).exists()
        keyword_segment_email_lists = SegmentKeyword.objects.filter(
            shared_with__contains=[user.email]).exists()
        if any([channel_segment_email_lists, video_segment_email_lists,
                keyword_segment_email_lists]):
            user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING)
