from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from brand_safety.models.bad_video import ALL_BAD_VIDEO_CATEGORIES
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class BadVideoCategoryListApiView(GenericAPIView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)

    def get(self, *_, **__):
        return Response(data=ALL_BAD_VIDEO_CATEGORIES)
