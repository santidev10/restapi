from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated

from es_components.countries import COUNTRY_CODES


class CountryListApiView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, *args, **kwargs):
        return Response(
            [
                {
                    "id": code,
                    "common": country
                }
                for country, code in COUNTRY_CODES.items()
            ]
        )
