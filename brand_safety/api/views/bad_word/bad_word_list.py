from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    queryset = BadWord.objects.all().order_by("name")

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name",)
    filter_fields = ("category__name",)
