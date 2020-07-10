from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from es_components.countries import COUNTRIES


class CountryListApiView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, *args, **kwargs):
        return Response(
            [
                {
                    "id": code,
                    "common": country[0]
                }
                for code, country in COUNTRIES.items()
            ]
        )
