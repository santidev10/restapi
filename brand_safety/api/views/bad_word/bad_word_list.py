from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.serializers import ValidationError

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadWord


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    pagination_class = BrandSafetyPaginator
    MIN_SEARCH_LENGTH = 3

    def do_filters(self, queryset):
        filters = {}

        search = self.request.query_params.get("search")
        if search:
            if len(search) < self.MIN_SEARCH_LENGTH:
                raise ValidationError("Search term must be at least {} characters.".format(self.MIN_SEARCH_LENGTH))
            filters["name__icontains"] = search

        category = self.request.query_params.get("category")
        if category:
            try:
                category_id = int(category)
                filters["category_id"] = category_id
            except ValueError:
                raise ValidationError("Category filter param must be Category ID value. Received: {}.".format(category))

        language = self.request.query_params.get("language")
        if language:
            filters["language__language"] = language

        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        queryset = BadWord.objects.all().order_by("name")
        queryset = self.do_filters(queryset)
        return queryset

    def post(self, request):
        serializer = BadWordSerializer(data=request.data, context={'request': request})
        if serializer.is_valid():
            try:
                data = serializer.validated_data
                existing_word = BadWord.all_objects.get(name=data["name"], category=data["category"], language=data["language"])
                existing_word.deleted_at = None
                existing_word.save()
            except BadWord.DoesNotExist:
                serializer.save()
            return Response(serializer.data, status=HTTP_201_CREATED)
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)
