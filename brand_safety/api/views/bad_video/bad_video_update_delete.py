from rest_framework.generics import RetrieveUpdateDestroyAPIView

from brand_safety.api.serializers.bad_video_serializer import BadVideoSerializer
from brand_safety.models import BadVideo
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class BadVideoUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)
    serializer_class = BadVideoSerializer
    queryset = BadVideo.objects.all()
