from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView

from brand_safety.api.serializers.bad_video_serializer import BadVideoSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadVideo
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class BadVideoListCreateApiView(ListCreateAPIView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)
    serializer_class = BadVideoSerializer
    pagination_class = BrandSafetyPaginator
    queryset = BadVideo.objects.all().order_by("title")

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("title",)
    filter_fields = ("category",)
