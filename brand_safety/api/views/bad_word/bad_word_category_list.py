from rest_framework.generics import GenericAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from brand_safety.models import BadWordCategory


class BadWordCategoryListApiView(GenericAPIView):
    permission_classes = (IsAdminUser,)

    def get(self, *_, **__):
        categories_list = BadWordCategory.objects.values("id", "name")
        return Response(data=categories_list)
