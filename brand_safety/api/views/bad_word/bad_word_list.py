from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord, BadWordCategory


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    queryset = BadWord.objects.all().order_by("name")

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name",)
    filter_fields = ("category__name",)

    def create(self, request):
        name = request.data.get("name")
        category = request.data.get("category")
        negative_score = request.data.get("negative_score")
        BadWord.objects.get_or_create(name=name, category=BadWordCategory.from_string(category), negative_score=negative_score)

        queryset = self.get_queryset()
        serializer = BadWordSerializer(queryset, many=True)
        return Response(serializer.data)


