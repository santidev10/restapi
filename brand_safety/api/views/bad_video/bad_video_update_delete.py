from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser

from brand_safety.api.serializers.bad_video_serializer import BadVideoSerializer
from brand_safety.models import BadVideo


class BadVideoUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadVideoSerializer
    queryset = BadVideo.objects.all()
