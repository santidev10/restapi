from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser

from brand_safety.api.serializers.bad_channel_serializer import BadChannelSerializer
from brand_safety.models import BadChannel


class BadChannelUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadChannelSerializer
    queryset = BadChannel.objects.all()
