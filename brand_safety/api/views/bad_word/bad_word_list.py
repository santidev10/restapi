from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadWord


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    pagination_class = BrandSafetyPaginator
    queryset = BadWord.objects.all().order_by("name")
