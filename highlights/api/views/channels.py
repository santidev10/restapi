from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from channel.api.views.channel_list import BaseChannelListApiView
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class HighlightChannelsListApiView(APIView,
                                   BaseChannelListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )

    max_pages_count = 5
    page_size = 20

    @add_brand_safety_data
    def get(self, request, *args, **kwargs):
        response_data = self._get_channel_list_data(request)
        return Response(response_data)
