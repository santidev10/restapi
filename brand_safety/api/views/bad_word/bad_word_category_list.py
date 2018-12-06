from rest_framework.generics import GenericAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from brand_safety.models import BadWord


class BadWordCategoryListApiView(GenericAPIView):
    permission_classes = (IsAdminUser,)

    def get(self, *_, **__):
        categories_list = BadWord.objects.values_list('category', flat=True).distinct()
        return Response(data=categories_list)
