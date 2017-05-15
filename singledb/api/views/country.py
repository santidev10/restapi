from rest_framework.generics import ListAPIView

from singledb.api.serializers import CountrySerializer
from singledb.models import Country


class CountryListApiView(ListAPIView):
    permission_classes = tuple()
    queryset = Country.objects.order_by("common")
    serializer_class = CountrySerializer
