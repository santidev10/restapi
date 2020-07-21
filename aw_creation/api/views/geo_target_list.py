from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.api.serializers import SimpleGeoTargetSerializer
from aw_reporting.models import GeoTarget


class GeoTargetListApiView(APIView):
    """
    Returns a list of geo-targets, limit is 100
    Accepts ?search=kharkiv parameter
    """
    queryset = GeoTarget.objects.all().order_by("name")
    serializer_class = SimpleGeoTargetSerializer

    def get(self, request, *args, **kwargs):
        queryset = self.queryset
        search = request.GET.get("search", "").strip()
        if search:
            queryset = queryset.filter(name__icontains=search)
        data = self.serializer_class(queryset[:100], many=True).data
        return Response(data=data)
