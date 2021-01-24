from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers import FlightSerializer
from aw_reporting.models import Flight
from userprofile.constants import StaticPermissions
from utils.views import get_object


class FlightAPIView(APIView):
    permission_classes = (StaticPermissions()(StaticPermissions.PACING_REPORT),)

    def patch(self, request, *args, **kwargs):
        flight = get_object(Flight, id=kwargs["pk"])
        data = request.data
        serializer = FlightSerializer(flight, data=data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(data=serializer.validated_data)
