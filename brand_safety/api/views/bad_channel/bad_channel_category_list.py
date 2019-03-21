from rest_framework.generics import GenericAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from brand_safety.models.bad_channel import ALL_BAD_CHANNEL_CATEGORIES


class BadChannelCategoryListApiView(GenericAPIView):
    permission_classes = (IsAdminUser,)

    def get(self, *_, **__):
        return Response(data=ALL_BAD_CHANNEL_CATEGORIES)
