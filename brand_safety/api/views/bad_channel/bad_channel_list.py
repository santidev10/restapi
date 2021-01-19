from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView

from brand_safety.api.serializers.bad_channel_serializer import BadChannelSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadChannel
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class BadChannelListApiView(ListCreateAPIView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)
    serializer_class = BadChannelSerializer
    pagination_class = BrandSafetyPaginator
    queryset = BadChannel.objects.all().order_by("title")

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("title",)
    filter_fields = ("category",)
