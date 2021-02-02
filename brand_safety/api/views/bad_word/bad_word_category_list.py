from distutils.util import strtobool

from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response

from brand_safety.api.serializers.bad_word_category_serializer import BadWordCategorySerializer
from brand_safety.models import BadWordCategory
from userprofile.constants import StaticPermissions


class BadWordCategoryListApiView(ListCreateAPIView):
    serializer_class = BadWordCategorySerializer
    queryset = BadWordCategory.objects.all()

    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name", "=id",)

    def list(self, request, *args, **kwargs):
        data = {}
        scoring_options = strtobool(request.query_params["scoring_options"]) \
            if "scoring_options" in request.query_params else False
        if request.user.has_permission(StaticPermissions.BSTE):
            queryset = self.get_queryset()
            serializer = self.get_serializer(queryset, many=True)
            if scoring_options:
                data["categories"] = serializer.data
                data["scoring_options"] = [
                    {"id": 1, "title": "1 - Low Risk"},
                    {"id": 2, "title": "2 - Medium Risk"},
                    {"id": 4, "title": "4 - High Risk"}
                ]
            else:
                data = serializer.data
        else:
            data = []
        return Response(data)
