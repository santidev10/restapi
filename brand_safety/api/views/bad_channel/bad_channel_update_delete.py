from rest_framework.generics import RetrieveUpdateDestroyAPIView

from brand_safety.api.serializers.bad_channel_serializer import BadChannelSerializer
from brand_safety.models import BadChannel
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class BadChannelUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)
    serializer_class = BadChannelSerializer
    queryset = BadChannel.objects.all()
