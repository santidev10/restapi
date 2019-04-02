from django_filters.rest_framework import DjangoFilterBackend
from django.db.utils import IntegrityError
from rest_framework import filters
from rest_framework.response import Response
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

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name",)
    filter_fields = ("category",)

    def post(self, request, *args, **kwargs):
        try:
            return self.create(request, *args, **kwargs)
        except IntegrityError:
            data = request.data
            return Response(status=200, data="Keyword {} with Category id {} already exists.".format(data.get("name"), data.get("category_ref_id")))