from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser

from brand_safety.api.serializers.bad_video_serializer import BadVideoSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadVideo


class BadVideoListCreateApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadVideoSerializer
    pagination_class = BrandSafetyPaginator
    queryset = BadVideo.objects.all().order_by("title")

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("title",)
    filter_fields = ("category",)
