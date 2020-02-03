from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response

from brand_safety.api.serializers.bad_word_category_serializer import BadWordCategorySerializer
from brand_safety.models import BadWordCategory


class BadWordCategoryListApiView(ListCreateAPIView):
    serializer_class = BadWordCategorySerializer
    queryset = BadWordCategory.objects.all()

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name", "=id",)

    def list(self, request, *args, **kwargs):
        data = {}
        if request.user.is_staff:
            queryset = self.get_queryset()
            serializer = self.get_serializer(queryset, many=True)
            data["categories"] = serializer.data
            data["scoring_options"] = {
                1: "Low Risk",
                2: "Medium Risk",
                4: "High Risk"
            }
        else:
            pass
        return Response(data)
