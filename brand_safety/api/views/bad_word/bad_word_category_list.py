from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.permissions import IsAdminUser
from rest_framework.generics import ListCreateAPIView

from brand_safety.api.serializers.bad_word_category_serializer import BadWordCategorySerializer
from brand_safety.models import BadWordCategory


class BadWordCategoryListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordCategorySerializer
    queryset = BadWordCategory.objects.all()

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name", "=id",)
