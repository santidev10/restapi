from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from video.api.views import BaseVideoListApiView


class HighlightVideosListApiView(APIView, BaseVideoListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )
    page_size = 20
    max_pages_count = 5
    allowed_aggregations = (
        "general_data.category",
        "general_data.language",
    )

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        video_list_data = self._get_video_list_data(request)
        return Response(video_list_data)
