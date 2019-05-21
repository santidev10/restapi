from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer

    def do_filters(self, queryset):
        filters = {}

        search = self.request.query_params.get("search")
        if search:
            filters["name__icontains"] = search

        category = self.request.query_params.get("category")
        if category:
            try:
                category_id = int(category)
            except ValueError:
                raise ValueError("Category filter param must be Category ID value. Received: {}.".format(category))

        if category:
            filters["category_id"] = category

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
            serializer.save()
            return Response(serializer.data, status=HTTP_201_CREATED)
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)