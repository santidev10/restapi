from rest_framework.generics import GenericAPIView
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.permissions import IsAdminUser
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response

<<<<<<< HEAD
from brand_safety.api.serializers.bad_word_category_serializer import BadWordCategorySerializer
=======
>>>>>>> 2583278aabef393f2b9cd722dfc70ca3793c3e8a
from brand_safety.models import BadWordCategory


class BadWordCategoryListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordCategorySerializer
    queryset = BadWordCategory.objects.all()

<<<<<<< HEAD
    filter_backends = (filters.SearchFilter, DjangoFilterBackend)
    search_fields = ("name", "=id",)
=======
    def get(self, *_, **__):
        categories_list = BadWordCategory.objects.values("id", "name")
        return Response(data=categories_list)
>>>>>>> 2583278aabef393f2b9cd722dfc70ca3793c3e8a
